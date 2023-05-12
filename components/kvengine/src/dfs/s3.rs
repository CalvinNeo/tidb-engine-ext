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
use engine_traits::{GetObjectOptions, ObjectStorage};
use farmhash::fingerprint64;
use futures::StreamExt;
use http::StatusCode;
use hyper_tls::HttpsConnector;
use regex::Regex;
use rusoto_core::{
    param::{Params, ServiceParams},
    request::{BufferedHttpResponse, HttpResponse},
    signature::SignedRequest,
    HttpClient, HttpDispatchError, Region, RusotoError,
};
use rusoto_s3::{
    CopyObjectError, DeleteObjectError, DeleteObjectTaggingError, GetObjectError,
    GetObjectTaggingError, ListObjectsV2Error, PutObjectError, PutObjectTaggingError,
};
use tikv_util::{box_err, time::Instant};
use tokio::runtime::Runtime;

use crate::dfs::{self, metrics::*, Dfs, Options};

const MAX_RETRY_COUNT: u32 = 9;
const RETRY_SLEEP_MS: u64 = 500;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(60);
const READ_BODY_TIMEOUT: Duration = Duration::from_secs(60);

pub const STORAGE_CLASS_DEFAULT: &str = "STANDARD";
pub const STORAGE_CLASS_STANDARD_IA: &str = "STANDARD_IA";

const AWS_DOMAIN_STRING: &str = "amazonaws";

const SMALL_FILE_THRESHOLD_BYTES: u64 = 1024 * 1024; // 1MB

#[derive(Clone)]
pub struct S3Fs {
    core: Arc<S3FsCore>,
}

impl S3Fs {
    pub fn new(
        prefix: String,
        endpoint: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
    ) -> Self {
        let core = Arc::new(S3FsCore::new(
            endpoint, key_id, secret_key, region, bucket, prefix,
        ));
        Self { core }
    }

    #[cfg(test)]
    pub fn new_for_test(s3c: rusoto_core::Client, bucket: String, prefix: String) -> Self {
        Self {
            core: Arc::new(S3FsCore::new_with_s3_client(
                s3c,
                "".to_string(),
                "local".to_string(),
                bucket,
                prefix,
            )),
        }
    }
}

impl Deref for S3Fs {
    type Target = S3FsCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct S3FsCore {
    s3c: rusoto_core::Client,
    hostname: String,
    region: Region,
    bucket: String,
    prefix: String,
    runtime: tokio::runtime::Runtime,
    virtual_host: bool,
}

impl S3FsCore {
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
        // local deployed s3 service like minio does not support virtual host
        // addressing, it always has a port defined at the end.
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

    pub fn file_key(&self, file_id: u64) -> String {
        let idx = (fingerprint64(file_id.to_le_bytes().as_slice())) as u8;
        format!("{}/{:02x}/{:016x}.sst", self.prefix, idx, file_id)
    }

    pub fn get_prefix(&self) -> String {
        self.prefix.clone()
    }

    pub fn is_on_aws(&self) -> bool {
        self.hostname.contains(AWS_DOMAIN_STRING)
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

    // Try to parse the sst file id from file key.
    // Expected file key format: "/{prefix}/{idx}/{file_id}.sst".
    // Note: do NOT use in performance critical path as regex is used.
    pub fn try_parse_file_id(&self, key: &str) -> Option<u64> {
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"/[0-9a-f]{2}/([0-9a-f]{16})\.sst$").unwrap();
        }
        let caps = RE.captures(key)?;
        Some(u64::from_str_radix(&caps[1], 16).unwrap())
    }

    fn is_err_retryable<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Service(_) => true,
            RusotoError::HttpDispatch(_) => true,
            RusotoError::InvalidDnsName(_) => false,
            RusotoError::Credentials(_) => false,
            RusotoError::Validation(_) => false,
            RusotoError::ParseError(_) => false,
            RusotoError::Unknown(resp) => resp.status.is_server_error(),
            RusotoError::Blocking => false,
        }
    }

    fn is_err_not_found<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Unknown(resp) => resp.status == StatusCode::NOT_FOUND,
            _ => false,
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

    /// list gets a list of file ids(full path) greater than `start_after`, with
    /// optional prefix `prefix`.
    ///
    /// The result contains:
    ///     A vector of file content with `key` & `last_modified` timestamp.
    ///     A boolean `has_more` indicate if there is more.
    ///     An optional `next_start_after` for next loop if `has_more` is true.
    ///
    /// Note:
    ///     `prefix` should NOT be contained in `start_after`.
    ///     The file ids in result are in full path, including `S3Fs.prefix` and
    /// `prefix`.
    ///
    /// Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    pub async fn list(
        &self,
        start_after: &str,
        prefix: Option<&str>,
    ) -> crate::dfs::Result<(
        Vec<ListObjectContent>,
        bool,           // has_more. Deprecated, use `next_start_after`
        Option<String>, // next_start_after
    )> {
        let prefix = format!("{}/{}", self.prefix.clone(), prefix.unwrap_or_default());
        let start_after = format!("{}{}", prefix, start_after);
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
                    let next_start_after = list.is_truncated.then(|| {
                        list.contents.last().unwrap().key.as_str()[prefix.len()..].to_string()
                    });
                    return Ok((list.contents, list.is_truncated, next_start_after));
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if self.is_err_not_found(&err) {
                return Ok((vec![], false, None));
            } else if self.is_err_retryable(&err) && retry_cnt < MAX_RETRY_COUNT {
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

    pub async fn is_removed(&self, file_id: u64) -> Result<bool, dfs::Error> {
        let mut retry_cnt = 0;
        loop {
            let key = self.file_key(file_id);
            let req = self.new_tagging_request("GET", &key);
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
                    return Ok(tagging.has_deleted_tag());
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(_) = err {
                return Err(dfs::Error::S3(format!(
                    "Get file {} tagging fail {:?}",
                    file_id, err
                )));
            }
            if self.is_err_retryable(&err) && retry_cnt < MAX_RETRY_COUNT {
                retry_cnt += 1;
                let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                warn!(
                    "Get file {} tagging fail {:?}, retry_cnt {}, retry after {}ms",
                    file_id, err, retry_cnt, retry_sleep
                );
                tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                continue;
            }
            return Err(dfs::Error::S3(format!(
                "Get file {} tagging, reach max retry count {}, err {:?}",
                file_id, MAX_RETRY_COUNT, err,
            )));
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

    fn new_tagging_request(&self, method: &str, key: &str) -> SignedRequest {
        let mut req = self.new_request(method, key);
        let mut params = Params::new();
        params.put_key("tagging");
        req.set_params(params);
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

    pub async fn get_object(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
    ) -> crate::dfs::Result<Bytes> {
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let mut req = self.new_request("GET", &key);
            if !opts.is_full_range() {
                req.add_header("Range", &format!("bytes={}", opts.range_string()));
            }
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

    // Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
    pub async fn delete_object(&self, key: String, file_name: String) -> crate::dfs::Result<()> {
        let mut retry_cnt = 0;
        let start_time = Instant::now();
        loop {
            let req = self.new_request("DELETE", &key);
            let result = self.dispatch(req, DeleteObjectError::from_response).await;
            if result.is_ok() {
                info!(
                    "delete file {}, takes {:?}, retry {}",
                    &file_name,
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                KVENGINE_DFS_LATENCY_VEC
                    .with_label_values(&["delete"])
                    .observe(start_time.saturating_elapsed().as_millis() as f64);
                return Ok(());
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < MAX_RETRY_COUNT {
                    KVENGINE_DFS_RETRY_COUNTER_VEC
                        .with_label_values(&["delete"])
                        .inc();
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    warn!(
                        "retry delete file {}, error {:?}, retry cnt {}, retry after {}ms",
                        &file_name, &err, retry_cnt, retry_sleep
                    );
                    continue;
                } else {
                    error!(
                        "delete file {}, takes {:?}, reach max retry count {}",
                        &file_name,
                        start_time.saturating_elapsed(),
                        MAX_RETRY_COUNT
                    );
                }
            }
            return Err(err.into());
        }
    }

    /// Copy object from `source_file_id` to `target_file_id`.
    ///
    /// `source_file_id` and `target_file_id` can be the same. And it's the only
    /// way to update the object creation timestamp, which is used for
    /// expiration rules of lifecycle.
    ///
    /// `target_tags`: replace tags if some, otherwise copy from source.
    ///
    /// `target_storage_class`: copy to another storage class if some. Note than
    /// only AWS S3 support storage class.
    pub async fn copy_object(
        &self,
        source_file_id: u64,
        target_file_id: u64,
        target_tagging: Option<&Tagging>,
        target_storage_class: Option<&str>,
    ) -> Result<(), dfs::Error> {
        let mut retry_cnt = 0;
        let mut copied = false;
        let source_key = format!("{}/{}", self.bucket, self.file_key(source_file_id));
        let target_key = self.file_key(target_file_id);

        loop {
            if !copied {
                let mut req = self.new_request("PUT", &target_key);
                req.add_header("x-amz-copy-source", &source_key);
                req.add_header("x-amz-metadata-directive", "REPLACE");
                if let Some(target_tagging) = target_tagging {
                    req.add_header("x-amz-tagging", &target_tagging.to_url_encoded());
                    req.add_header("x-amz-tagging-directive", "REPLACE");
                }
                if let Some(target_storage_class) = target_storage_class.as_ref() {
                    if self.is_on_aws() {
                        req.add_header("x-amz-storage-class", target_storage_class);
                    } else {
                        debug!(
                            "{} ignore target_storage_class {} which is not supported by {}",
                            target_file_id, target_storage_class, self.hostname
                        );
                    }
                }
                if let Err(err) = self.dispatch(req, CopyObjectError::from_response).await {
                    if retry_cnt < MAX_RETRY_COUNT {
                        retry_cnt += 1;
                        let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                        warn!(
                            "retry copy file {}, retry count {}, retry after {}ms, err {:?}",
                            target_file_id, retry_cnt, retry_sleep, err,
                        );
                        tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                        continue;
                    } else {
                        let err_msg = format!(
                            "failed to copy file {} from {}, reach max retry count {}, err {:?}",
                            target_file_id, source_file_id, MAX_RETRY_COUNT, err,
                        );
                        error!("{}", err_msg);
                        return Err(dfs::Error::S3(err_msg));
                    }
                }
                copied = true;
            }

            if target_tagging.is_none() || !self.hostname.contains("ksyuncs.com") {
                return Ok(());
            }

            // ks3 doesn't support copy object with tagging, workaround to send another
            // request. TODO: remove it when KS3 fixed the compatibility issue.
            let target_tagging = target_tagging.unwrap();
            let mut req = self.new_tagging_request("PUT", &target_key);
            let tagging_xml = target_tagging.to_xml();
            let stream = futures::stream::once(async move { Ok(Bytes::from(tagging_xml)) });
            req.set_content_type("application/xml".to_string());
            req.set_payload_stream(rusoto_core::ByteStream::new(stream));
            if let Err(err) = self
                .dispatch(req, PutObjectTaggingError::from_response)
                .await
            {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                    warn!("retry tagging file {}, error {:?}", target_file_id, &err);
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    continue;
                } else {
                    let err_msg = format!(
                        "failed to tagging file {}, reach max retry count {}, err {:?}",
                        target_file_id, MAX_RETRY_COUNT, err,
                    );
                    error!("{}", err_msg);
                    return Err(dfs::Error::S3(err_msg));
                }
            }
            return Ok(());
        }
    }

    async fn _remove_file_tagging(&self, file_id: u64) -> Result<(), dfs::Error> {
        let mut retry_cnt = 0;
        loop {
            let key = self.file_key(file_id);
            let req = self.new_tagging_request("DELETE", &key);
            if let Err(err) = self
                .dispatch(req, DeleteObjectTaggingError::from_response)
                .await
            {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    warn!(
                        "{}nd retry remove file {} tag error {:?}",
                        retry_cnt, file_id, &err
                    );
                    let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    continue;
                } else {
                    return Err(dfs::Error::S3(format!(
                        "Failed to remove file {} tag, reach max retry count {}, err {:?}",
                        file_id, MAX_RETRY_COUNT, err,
                    )));
                }
            }
            return Ok(());
        }
    }

    pub async fn retain_file(&self, file_id: u64) -> Result<(), dfs::Error> {
        if self.is_removed(file_id).await? {
            let empty_tagging = Tagging::default();
            self.copy_object(
                file_id,
                file_id,
                Some(&empty_tagging),
                Some(STORAGE_CLASS_DEFAULT),
            )
            .await?;
        }
        Ok(())
    }

    /// Choose proper storage class for removed files.
    pub fn choose_storage_class_for_removed_files(&self, file_len: Option<u64>) -> &'static str {
        // STORAGE_CLASS_STANDARD_IA is more cost efficient than STORAGE_CLASS_STANDARD
        // for NOT small files.
        if file_len.is_some() && file_len.unwrap() > SMALL_FILE_THRESHOLD_BYTES {
            STORAGE_CLASS_STANDARD_IA
        } else {
            STORAGE_CLASS_DEFAULT
        }
    }
}

impl ObjectStorage for S3Fs {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> Result<(), String> {
        let runtime = self.get_runtime();
        let len = objects.len();
        let mut handles = Vec::with_capacity(len);
        for (key, data) in objects {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            handles.push(runtime.spawn(async move {
                fs.put_object(full_key, data, key.clone())
                    .await
                    .map_err(|err| format!("put {} failed {:?}", &key, err))
            }));
        }

        let errs: Vec<_> = runtime
            .block_on(futures::future::join_all(handles))
            .into_iter()
            .filter_map(|r| r.unwrap().err())
            .collect();
        if !errs.is_empty() {
            return Err(format!("{:?}", errs));
        }
        Ok(())
    }

    fn get_objects(
        &self,
        keys: Vec<(String, GetObjectOptions)>,
    ) -> Result<Vec<(String, Bytes)>, String> {
        let runtime = self.get_runtime();
        let len = keys.len();
        let mut handles = Vec::with_capacity(len);
        for (key, opts) in keys {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            handles.push(runtime.spawn(async move {
                fs.get_object(full_key, key.clone(), opts)
                    .await
                    .map_err(|err| format!("put {} failed {:?}", &key, err))
                    .map(|data| (key.clone(), data))
            }));
        }

        let mut objects = vec![];
        let mut errs = vec![];
        for res in runtime.block_on(futures::future::join_all(handles)) {
            match res.unwrap() {
                Ok((key, data)) => {
                    objects.push((key, data));
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if !errs.is_empty() {
            return Err(format!("{:?}", errs));
        }
        Ok(objects)
    }
}

#[async_trait]
impl Dfs for S3Fs {
    async fn read_file(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<Bytes> {
        self.get_object(
            self.file_key(file_id),
            file_id.to_string(),
            GetObjectOptions::default(),
        )
        .await
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> crate::dfs::Result<()> {
        self.put_object(self.file_key(file_id), data, file_id.to_string())
            .await
    }

    /// Logically remove the file on S3 by tagging with "deleted=true".
    /// And the file would be permanently removed after `gc_lifetime`. See
    /// `DfsGc`.
    async fn remove(&self, file_id: u64, file_len: Option<u64>, _opts: Options) {
        // TODO: Reuse `copy_object` method.

        // Only AWS supports storage class.
        let target_storage_class = if self.is_on_aws() {
            let new_storage_class = self.choose_storage_class_for_removed_files(file_len);
            (new_storage_class != STORAGE_CLASS_DEFAULT).then_some(new_storage_class)
        } else {
            None
        };

        let mut retry_cnt = 0;
        let mut copied = false;
        loop {
            let key = self.file_key(file_id);
            // Copy object to update the creation time, as the s3 clean policy
            // depends on the creation time.
            if !copied {
                let mut req = self.new_request("PUT", &key);
                req.add_header("x-amz-copy-source", &format!("{}/{}", self.bucket, key));
                req.add_header("x-amz-metadata-directive", "REPLACE");
                req.add_header("x-amz-tagging", "deleted=true");
                req.add_header("x-amz-tagging-directive", "REPLACE");
                if let Some(target_storage_class) = target_storage_class {
                    req.add_header("x-amz-storage-class", target_storage_class);
                }
                if let Err(err) = self.dispatch(req, CopyObjectError::from_response).await {
                    if retry_cnt < MAX_RETRY_COUNT {
                        retry_cnt += 1;
                        let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
                        warn!(
                            "retry remove file {}, retry count {}, retry after {}ms, err {:?}",
                            file_id, retry_cnt, retry_sleep, err,
                        );
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
            if !self.hostname.contains("ksyuncs.com") {
                return;
            }
            // ks3 doesn't support copy object with tagging, workaround to send another
            // request. TODO: remove it when KS3 fixed the compatibility issue.
            let mut req = self.new_tagging_request("PUT", &key);
            let tagging = Tagging::new_single_deleted();
            let tagging_xml = quick_xml::se::to_string(&tagging).unwrap();
            let stream = futures::stream::once(async move { Ok(Bytes::from(tagging_xml)) });
            req.set_content_type("application/xml".to_string());
            req.set_payload_stream(rusoto_core::ByteStream::new(stream));
            if let Err(err) = self
                .dispatch(req, PutObjectTaggingError::from_response)
                .await
            {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * RETRY_SLEEP_MS;
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

    /// Permanently remove the file on S3.
    /// This method should be used by `DfsGc` ONLY to meet GC rules.
    async fn permanently_remove(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<()> {
        if self.is_on_aws() {
            return Err(box_err!(
                "{} permanently_remove is forbidden on AWS",
                file_id
            ));
        }

        self.delete_object(self.file_key(file_id), file_id.to_string())
            .await
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

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectContent {
    pub key: String,
    pub last_modified: String,
    pub storage_class: String,
    pub size: u64, // in bytes.
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct Tagging {
    tag_set: TagSet,
}

impl Tagging {
    fn new_single_deleted() -> Tagging {
        Self {
            tag_set: TagSet {
                tag: vec![Tag::new_deleted()],
            },
        }
    }

    pub fn add_tag(&mut self, key: String, value: String) {
        self.tag_set.tag.push(Tag { key, value });
    }

    fn has_deleted_tag(&self) -> bool {
        self.tag_set.tag.iter().any(|tag| tag.is_deleted())
    }

    pub fn to_xml(&self) -> String {
        quick_xml::se::to_string(&self).unwrap()
    }

    pub fn to_url_encoded(&self) -> String {
        let mut se = url::form_urlencoded::Serializer::new(String::new());
        for tag in &self.tag_set.tag {
            se.append_pair(&tag.key, &tag.value);
        }
        se.finish()
    }

    pub fn from_url_encoded(query: &str) -> Tagging {
        let mut tagging = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            tagging.tag_set.tag.push(Tag {
                key: key.into_owned(),
                value: value.into_owned(),
            });
        }
        tagging
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct TagSet {
    tag: Vec<Tag>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct Tag {
    #[serde(rename = "$unflatten=Key")]
    key: String,
    #[serde(rename = "$unflatten=Value")]
    value: String,
}

impl Tag {
    fn new_deleted() -> Self {
        Self {
            key: "deleted".to_string(),
            value: "true".to_string(),
        }
    }

    fn is_deleted(&self) -> bool {
        self.key == "deleted" && self.value == "true"
    }
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

    fn new_s3fs(file_data: &[u8]) -> S3Fs {
        let s3c = rusoto_core::Client::new_with(
            MockCredentialsProvider,
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200)
                    .with_body(str::from_utf8(file_data).unwrap()),
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200),
            ]),
        );
        S3Fs::new_for_test(s3c, "shard-db".into(), "prefix".into())
    }

    #[test]
    fn test_s3() {
        crate::tests::init_logger();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let s3fs = new_s3fs(&file_data);
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
        let file = LocalFile::open(321, &local_file, false).unwrap();
        assert_eq!(file.size(), 8u64);
        assert_eq!(file.id(), 321u64);
        let data = file.read(0, 8).unwrap();
        assert_eq!(&data, &file_data);
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(321, None, Options::new(1, 1)).await;
            tx.send(true).unwrap();
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let _ = fs::remove_file(local_file);
    }

    #[test]
    fn test_parse_sst_file() {
        let s3fs = new_s3fs(b"abcdefgh");

        let file_key = s3fs.file_key(random());
        assert_eq!(
            format!("{}/{}", "prefix", s3fs.parse_sst_file_suffix(&file_key)),
            file_key
        );

        for file_id in [0, 42, 0x1_0000_0000, 0xffff_ffff_ffff_ffff] {
            let file_key = s3fs.file_key(file_id);
            assert_eq!(s3fs.try_parse_file_id(&file_key), Some(file_id));
        }

        for file_key in [
            "",
            "cse/0000000000000001/e00000001/0000000000800000_00000000008e9000.wal",
        ] {
            assert_eq!(s3fs.try_parse_file_id(file_key), None);
        }
    }

    #[test]
    fn test_tagging() {
        let tagging_deleted = Tagging::new_single_deleted();

        assert_eq!(
            tagging_deleted.to_xml(),
            "<Tagging><TagSet><Tag><Key>deleted</Key><Value>true</Value></Tag></TagSet></Tagging>"
        );
        assert_eq!(tagging_deleted.to_url_encoded(), "deleted=true");
        assert_eq!(
            tagging_deleted.tag_set.tag,
            Tagging::from_url_encoded("deleted=true").tag_set.tag
        );
    }
}
