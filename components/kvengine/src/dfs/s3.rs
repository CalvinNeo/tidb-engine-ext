// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bstr::ByteSlice;
use bytes::{Buf, Bytes};
use engine_traits::ObjectStorage;
use farmhash::fingerprint64;
use futures::StreamExt;
use hyper_tls::HttpsConnector;
use rusoto_core::{
    param::{Params, ServiceParams},
    request::{BufferedHttpResponse, HttpResponse},
    signature::SignedRequest,
    HttpClient, HttpDispatchError, Region, RusotoError,
};
use rusoto_s3::{
    CopyObjectError, GetObjectError, GetObjectTaggingError, ListObjectsV2Error, PutObjectError,
    PutObjectTaggingError,
};
use tikv_util::time::Instant;
use tokio::runtime::Runtime;

use crate::dfs::{metrics::*, Options, DFS};

const MAX_RETRY_COUNT: u32 = 7;
const RETRY_SLEEP_MS: u64 = 500;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(60);
const READ_BODY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct S3FS {
    core: Arc<S3FSCore>,
}

impl S3FS {
    pub fn new(
        prefix: String,
        endpoint: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
    ) -> Self {
        let core = Arc::new(S3FSCore::new(
            endpoint, key_id, secret_key, region, bucket, prefix,
        ));
        Self { core }
    }

    #[cfg(test)]
    pub fn new_for_test(s3c: rusoto_core::Client, bucket: String, prefix: String) -> Self {
        Self {
            core: Arc::new(S3FSCore::new_with_s3_client(
                s3c,
                "".to_string(),
                "local".to_string(),
                bucket,
                prefix,
            )),
        }
    }
}

impl Deref for S3FS {
    type Target = S3FSCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct S3FSCore {
    s3c: rusoto_core::Client,
    hostname: String,
    region: Region,
    bucket: String,
    prefix: String,
    runtime: tokio::runtime::Runtime,
    virtual_host: bool,
}

impl S3FSCore {
    pub fn new(
        endpoint: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
        prefix: String,
    ) -> Self {
        let mut config = rusoto_core::HttpConfig::new();
        config.read_buf_size(256 * 1024);
        let use_tls = endpoint.starts_with("https");
        let default_provider = aws::CredentialsProvider::new().unwrap();
        let static_provider =
            rusoto_credential::StaticProvider::new(key_id.clone(), secret_key, None, None);
        let mut http_connector = hyper::client::connect::HttpConnector::new();
        http_connector.set_connect_timeout(Some(CONNECTION_TIMEOUT));
        let s3c = if use_tls {
            let https_connector = HttpsConnector::new_with_connector(http_connector);
            let http_client = HttpClient::from_connector_with_config(https_connector, config);
            if key_id.is_empty() {
                rusoto_core::Client::new_with(default_provider, http_client)
            } else {
                rusoto_core::Client::new_with(static_provider, http_client)
            }
        } else {
            let http_client = HttpClient::from_connector_with_config(http_connector, config);
            if key_id.is_empty() {
                rusoto_core::Client::new_with(default_provider, http_client)
            } else {
                rusoto_core::Client::new_with(static_provider, http_client)
            }
        };
        let endpoint = if endpoint.is_empty() {
            format!("http://s3.{}.amazonaws.com", region.as_str())
        } else {
            endpoint
        };
        Self::new_with_s3_client(s3c, endpoint, region, bucket, prefix)
    }

    pub fn new_with_s3_client(
        s3c: rusoto_core::Client,
        endpoint: String,
        region: String,
        bucket: String,
        mut prefix: String,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("s3")
            .build()
            .unwrap();
        if prefix.is_empty() {
            prefix.push_str("default")
        }
        let no_schema_endpoint = endpoint
            .find("://")
            .map(|p| &endpoint[p + 3..])
            .unwrap_or(&endpoint);
        // local deployed s3 service like minio does not support virtual host addressing, it always
        // has a port defined at the end.
        let virtual_host = !no_schema_endpoint.contains(':');
        let hostname = if virtual_host {
            format!("{}.{}", &bucket, no_schema_endpoint)
        } else {
            no_schema_endpoint.to_string()
        };
        let region = Region::Custom {
            name: region,
            endpoint,
        };
        Self {
            s3c,
            hostname,
            region,
            bucket,
            prefix,
            runtime,
            virtual_host,
        }
    }

    fn file_key(&self, file_id: u64) -> String {
        let idx = (fingerprint64(file_id.to_le_bytes().as_slice())) as u8;
        format!("{}/{:02x}/{:016x}.sst", self.prefix, idx, file_id)
    }

    pub fn get_prefix(&self) -> String {
        self.prefix.clone()
    }

    // parse the sst file's suffix with format {idx}/{file_id}.sst
    pub fn parse_sst_file_suffix(&self, key: &str) -> String {
        let end_idx = key.len();
        let start_idx = end_idx - 4 - 16 - 1 - 2;
        let suffix = &key[start_idx..end_idx];
        suffix.to_string()
    }

    pub fn parse_file_id(&self, key: &str) -> u64 {
        let end_idx = key.len() - 4;
        let start_idx = end_idx - 16;
        let file_part = &key[start_idx..end_idx];
        u64::from_str_radix(file_part, 16).unwrap()
    }

    fn is_err_retryable<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Service(_) => true,
            RusotoError::HttpDispatch(_) => true,
            RusotoError::InvalidDnsName(_) => false,
            RusotoError::Credentials(_) => false,
            RusotoError::Validation(_) => false,
            RusotoError::ParseError(_) => false,
            RusotoError::Unknown(_) => true,
            RusotoError::Blocking => false,
        }
    }

    async fn sleep_for_retry(&self, retry_cnt: &mut u32, file_name: &str) -> bool {
        if *retry_cnt < MAX_RETRY_COUNT {
            *retry_cnt += 1;
            let retry_sleep = 2u64.pow(*retry_cnt) * RETRY_SLEEP_MS;
            tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
            true
        } else {
            error!(
                "read file {}, reach max retry count {}",
                file_name, MAX_RETRY_COUNT
            );
            false
        }
    }

    // list gets a list of file ids(full path) greater than start_after.
    // The result contains a vector of file ids and a boolean indicate if there is more.
    pub async fn list(&self, start_after: &str) -> crate::dfs::Result<(Vec<String>, bool)> {
        let prefix = format!("{}/", self.prefix.clone());
        let start_after = format!("{}/{}", self.prefix.clone(), start_after);
        let mut retry_cnt = 0;
        loop {
            let mut req = self.new_request("GET", "");
            let mut params = Params::new();
            params.put("list-type", "2");
            params.put("start-after", &start_after);
            params.put("prefix", &prefix);
            req.set_params(params);
            let mut result = self.dispatch(req, ListObjectsV2Error::from_response).await;
            if result.is_ok() {
                let response = result.unwrap();
                let body_res = self.read_body(response).await;
                if body_res.is_ok() {
                    let body = body_res.unwrap();
                    let body_str = body.to_str().unwrap();
                    let list: ListObjects = quick_xml::de::from_str(body_str).unwrap();
                    let mut files = vec![];
                    for content in list.contents {
                        files.push(content.key);
                    }
                    return Ok((files, list.is_truncated));
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) && retry_cnt < MAX_RETRY_COUNT {
                retry_cnt += 1;
                let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                continue;
            }
            error!(
                "failed to list files start after {}, reach max retry count {}, err {:?}",
                start_after, MAX_RETRY_COUNT, err,
            );
            return Err(err.into());
        }
    }

    pub async fn is_removed(&self, file_id: u64) -> bool {
        let mut retry_cnt = 0;
        loop {
            let key = self.file_key(file_id);
            let mut req = self.new_request("GET", &key);
            let mut params = Params::new();
            params.put_key("tagging");
            req.set_params(params);
            let mut result = self
                .dispatch(req, GetObjectTaggingError::from_response)
                .await;
            if result.is_ok() {
                let resp = result.unwrap();
                let body_res = self.read_body(resp).await;
                if body_res.is_ok() {
                    let body = body_res.unwrap();
                    let body_str = body.to_str().unwrap();
                    let tagging: Tagging = quick_xml::de::from_str(body_str).unwrap();
                    return tagging
                        .tag_set
                        .tag
                        .iter()
                        .any(|tag| tag.key == "deleted" && tag.value == "true");
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(_) = err {
                return true;
            }
            if self.is_err_retryable(&err) && retry_cnt < MAX_RETRY_COUNT {
                retry_cnt += 1;
                let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                continue;
            }
            error!(
                "failed to get tagging for file {}, reach max retry count {}, err {:?}",
                file_id, MAX_RETRY_COUNT, err,
            );
            return true;
        }
    }

    fn new_request(&self, method: &str, key: &str) -> SignedRequest {
        let path = if self.virtual_host {
            format!("/{}", key)
        } else {
            format!("/{}/{}", &self.bucket, key)
        };
        let mut req = SignedRequest::new(method, "s3", &self.region, &path);
        req.scheme = Some("http".to_string());
        req
    }

    async fn dispatch<E>(
        &self,
        mut req: SignedRequest,
        from_response: fn(BufferedHttpResponse) -> RusotoError<E>,
    ) -> Result<Response, RusotoError<E>> {
        req.set_hostname(Some(self.hostname.clone()));
        let mut resp = self
            .s3c
            .sign_and_dispatch_timeout(req, DISPATCH_TIMEOUT)
            .await?;
        if !resp.status.is_success() {
            let buffered = resp.buffer().await.map_err(RusotoError::HttpDispatch)?;
            return Err(from_response(buffered));
        }
        Ok(Response { resp })
    }

    async fn read_body(&self, mut resp: Response) -> Result<Bytes, HttpDispatchError> {
        let cap = resp
            .headers
            .remove("Content-Length")
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or_default();
        let mut buf = Vec::with_capacity(cap);
        while let Some(res) = tokio::time::timeout(READ_BODY_TIMEOUT, resp.body.next())
            .await
            .map_err(|e| HttpDispatchError::new(format!("read body timeout {:?}", e)))?
        {
            let chunk = res.map_err(|e| HttpDispatchError::new(format!("{:?}", e)))?;
            buf.extend_from_slice(chunk.chunk());
        }
        Ok(Bytes::from(buf))
    }

    pub async fn get_object(&self, key: String, file_name: String) -> crate::dfs::Result<Bytes> {
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let req = self.new_request("GET", &key);
            let mut result = self.dispatch(req, GetObjectError::from_response).await;

            if result.is_ok() {
                let resp = result.unwrap();
                let body = self.read_body(resp).await;
                match body {
                    Ok(data) => {
                        info!(
                            "read file {}, size {}, takes {:?}, retry {}",
                            &file_name,
                            data.len(),
                            start_time.saturating_elapsed(),
                            retry_cnt
                        );
                        KVENGINE_DFS_THROUGHPUT_VEC
                            .with_label_values(&["read"])
                            .inc_by(data.len() as u64);
                        KVENGINE_DFS_LATENCY_VEC
                            .with_label_values(&["read"])
                            .observe(start_time.saturating_elapsed().as_millis() as f64);
                        return Ok(data);
                    }
                    Err(err) => result = Err(err.into()),
                }
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(GetObjectError::NoSuchKey(err_msg)) = &err {
                error!("file {} not exist, err msg {}", &file_name, err_msg);
                return Err(err.into());
            }
            if self.is_err_retryable(&err) && self.sleep_for_retry(&mut retry_cnt, &file_name).await
            {
                KVENGINE_DFS_RETRY_COUNTER_VEC
                    .with_label_values(&["read"])
                    .inc();
                warn!("retry read file {}, error {:?}", &file_name, &err);
                continue;
            }
            return Err(err.into());
        }
    }

    pub async fn put_object(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
    ) -> crate::dfs::Result<()> {
        let mut retry_cnt = 0;
        let start_time = Instant::now();
        let data_len = data.len();
        loop {
            let mut req = self.new_request("PUT", &key);
            req.add_header("Content-Length", &format!("{}", data.len()));
            let data = data.clone();
            let stream = futures::stream::once(async move { Ok(data) });
            req.set_payload_stream(rusoto_core::ByteStream::new(stream));
            let result = self.dispatch(req, PutObjectError::from_response).await;
            if result.is_ok() {
                info!(
                    "create file {}, size {}, takes {:?}, retry {}",
                    &file_name,
                    data_len,
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                KVENGINE_DFS_THROUGHPUT_VEC
                    .with_label_values(&["write"])
                    .inc_by(data_len as u64);
                KVENGINE_DFS_LATENCY_VEC
                    .with_label_values(&["write"])
                    .observe(start_time.saturating_elapsed().as_millis() as f64);
                return Ok(());
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < MAX_RETRY_COUNT {
                    KVENGINE_DFS_RETRY_COUNTER_VEC
                        .with_label_values(&["write"])
                        .inc();
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    warn!("retry create file {}, error {:?}", &file_name, &err);
                    continue;
                } else {
                    error!(
                        "create file {}, takes {:?}, reach max retry count {}",
                        &file_name,
                        start_time.saturating_elapsed(),
                        MAX_RETRY_COUNT
                    );
                }
            }
            return Err(err.into());
        }
    }
}

impl ObjectStorage for S3FS {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> engine_traits::Result<()> {
        let runtime = self.get_runtime();
        let len = objects.len();
        let (tx, rx) = tikv_util::mpsc::bounded(len);
        for (key, data) in objects {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            let tx = tx.clone();
            runtime.spawn(async move {
                let result = fs
                    .put_object(full_key, data, key.clone())
                    .await
                    .map_err(|err| format!("put {} failed {:?}", &key, err));
                tx.send(result).unwrap();
            });
        }
        let mut errs = vec![];
        for _ in 0..len {
            if let Err(err) = rx.recv().unwrap() {
                errs.push(err)
            }
        }
        if errs.len() > 0 {
            return Err(engine_traits::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", errs),
            )));
        }
        Ok(())
    }

    fn get_objects(&self, keys: Vec<String>) -> engine_traits::Result<Vec<(String, Bytes)>> {
        let runtime = self.get_runtime();
        let len = keys.len();
        let (tx, rx) = tikv_util::mpsc::bounded(len);
        let mut objects = vec![];
        for key in keys {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            let tx = tx.clone();
            runtime.spawn(async move {
                let result = fs
                    .get_object(full_key, key.clone())
                    .await
                    .map_err(|err| format!("put {} failed {:?}", &key, err))
                    .map(|data| (key.clone(), data));
                tx.send(result).unwrap();
            });
        }
        let mut errs = vec![];
        for _ in 0..len {
            match rx.recv().unwrap() {
                Ok((key, data)) => {
                    objects.push((key, data));
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if errs.len() > 0 {
            return Err(engine_traits::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", errs),
            )));
        }
        Ok(objects)
    }
}

#[async_trait]
impl DFS for S3FS {
    async fn read_file(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<Bytes> {
        self.get_object(self.file_key(file_id), file_id.to_string())
            .await
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> crate::dfs::Result<()> {
        self.put_object(self.file_key(file_id), data, file_id.to_string())
            .await
    }

    async fn remove(&self, file_id: u64, _opts: Options) {
        let mut retry_cnt = 0;
        let mut copied = false;
        loop {
            let key = self.file_key(file_id);
            // copy object
            if !copied {
                let mut req = self.new_request("PUT", &key);
                req.add_header("x-amz-copy-source", &format!("{}/{}", self.bucket, key));
                req.add_header("x-amz-metadata-directive", "REPLACE");
                if let Err(err) = self.dispatch(req, CopyObjectError::from_response).await {
                    if retry_cnt < MAX_RETRY_COUNT {
                        retry_cnt += 1;
                        let retry_sleep = 2u64.pow(retry_cnt as u32) * RETRY_SLEEP_MS;
                        tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                        continue;
                    } else {
                        error!(
                            "failed to remove file {}, reach max retry count {}, err {:?}",
                            file_id, MAX_RETRY_COUNT, err,
                        );
                        return;
                    }
                }
                copied = true;
            }
            // put tagging
            let mut req = self.new_request("PUT", &key);
            let mut params = Params::new();
            params.put_key("tagging");
            req.set_params(params);
            req.set_content_type("application/xml".to_string());
            let tagging = Tagging::new_single("deleted", "true");
            let tagging_xml = quick_xml::se::to_string(&tagging).unwrap();
            let stream = futures::stream::once(async move { Ok(Bytes::from(tagging_xml)) });
            req.set_payload_stream(rusoto_core::ByteStream::new(stream));
            if let Err(err) = self
                .dispatch(req, PutObjectTaggingError::from_response)
                .await
            {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt as u32) * RETRY_SLEEP_MS;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    warn!("retry remove file {}, error {:?}", file_id, &err);
                    continue;
                } else {
                    error!(
                        "failed to remove file {}, reach max retry count {}, err {:?}",
                        file_id, MAX_RETRY_COUNT, err,
                    );
                }
            }
            return;
        }
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct ListObjects {
    contents: Vec<ListObjectContent>,
    is_truncated: bool,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct ListObjectContent {
    key: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct Tagging {
    tag_set: TagSet,
}

impl Tagging {
    fn new_single(key: &str, value: &str) -> Tagging {
        Self {
            tag_set: TagSet {
                tag: vec![Tag {
                    key: key.to_string(),
                    value: value.to_string(),
                }],
            },
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct TagSet {
    tag: Vec<Tag>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct Tag {
    #[serde(rename = "$unflatten=Key")]
    key: String,
    #[serde(rename = "$unflatten=Value")]
    value: String,
}

struct Response {
    resp: HttpResponse,
}

impl Deref for Response {
    type Target = HttpResponse;

    fn deref(&self) -> &Self::Target {
        &self.resp
    }
}

impl DerefMut for Response {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resp
    }
}

impl Debug for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resp.status)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, str};

    use bytes::Buf;
    use rand::random;
    use rusoto_mock::{
        MockCredentialsProvider, MockRequestDispatcher, MultipleMockRequestDispatcher,
    };

    use super::*;
    use crate::table::sstable::{new_filename, File, LocalFile};

    #[test]
    fn test_s3() {
        crate::tests::init_logger();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();

        let s3c = rusoto_core::Client::new_with(
            MockCredentialsProvider,
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200)
                    .with_body(str::from_utf8(&file_data).unwrap()),
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200),
            ]),
        );
        let s3fs = S3FS::new_for_test(s3c, "shard-db".into(), "prefix".into());
        let (tx, rx) = tikv_util::mpsc::bounded(1);

        let fs = s3fs.clone();
        let file_data2 = file_data.clone();
        let f = async move {
            match fs
                .create(321, bytes::Bytes::from(file_data2), Options::new(1, 1))
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let local_file = new_filename(321, local_dir.path());
        let move_local_file = local_file.clone();
        let f = async move {
            let opts = Options::new(1, 1);
            match fs.read_file(321, opts).await {
                Ok(data) => {
                    let mut file = std::fs::File::create(&move_local_file).unwrap();
                    file.write_all(data.chunk()).unwrap();
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let data = std::fs::read(&local_file).unwrap();
        assert_eq!(&data, &file_data);
        let file = LocalFile::open(321, &local_file).unwrap();
        assert_eq!(file.size(), 8u64);
        assert_eq!(file.id(), 321u64);
        let data = file.read(0, 8).unwrap();
        assert_eq!(&data, &file_data);
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(321, Options::new(1, 1)).await;
            tx.send(true).unwrap();
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let _ = fs::remove_file(local_file);
    }

    #[test]
    fn test_parse_sst_file_suffix() {
        let file_data = "abcdefgh".to_string().into_bytes();
        let s3c = rusoto_core::Client::new_with(
            MockCredentialsProvider,
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200)
                    .with_body(str::from_utf8(&file_data).unwrap()),
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200),
            ]),
        );
        let s3fs = S3FS::new_for_test(s3c, "shard-db".into(), "prefix".into());
        let file_key = s3fs.file_key(random());
        assert_eq!(
            format!("{}/{}", "prefix", s3fs.parse_sst_file_suffix(&file_key)),
            file_key
        );
    }
}
