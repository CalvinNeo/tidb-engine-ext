// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::time::Instant;

use kvproto::backup::StorageBackend;
use kvproto::import_sstpb::*;
use uuid::Uuid;

use crate::storage::mvcc::Write;
use crate::storage::Key;
use engine::rocks::util::{get_cf_handle, prepare_sst_for_ingestion, validate_sst_for_ingestion};
use engine::rocks::{IngestExternalFileOptions, SeekKey, SstReader, SstWriter, DB};
use engine::CF_WRITE;
use external_storage::{block_on_external_io, create_storage, url_of_backend};
use futures_util::io::{copy, AllowStdIo};
use tikv_util::time::Limiter;

use super::metrics::*;
use super::{Error, Result};
use crate::raftstore::store::keys;
use crate::tiflash_ffi::invoke;
use crate::tiflash_ffi::invoke::gen_snap_kv_data_from_sst;

/// SSTImporter manages SST files that are waiting for ingesting.
pub struct SSTImporter {
    dir: ImportDir,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<SSTImporter> {
        Ok(SSTImporter {
            dir: ImportDir::new(root)?,
        })
    }

    pub fn get_path(&self, meta: &SSTMeta) -> PathBuf {
        let path = self.dir.join(meta).unwrap();
        path.save.clone()
    }

    pub fn create(&self, meta: &SSTMeta) -> Result<ImportFile> {
        match self.dir.create(meta) {
            Ok(f) => {
                info!("create"; "file" => ?f);
                Ok(f)
            }
            Err(e) => {
                error!("create failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn delete(&self, meta: &SSTMeta) -> Result<()> {
        match self.dir.delete(meta) {
            Ok(path) => {
                info!("delete"; "path" => ?path);
                Ok(())
            }
            Err(e) => {
                error!("delete failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn ingest(&self, meta: &SSTMeta, db: &DB) -> Result<()> {
        match self.dir.ingest(meta, db) {
            Ok(_) => {
                info!("ingest"; "meta" => ?meta);
                Ok(())
            }
            Err(e) => {
                error!("ingest failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn gen_snapshot_from_sst(&self, meta: &SSTMeta, db: &DB) -> Result<invoke::SnapshotKV> {
        match self.dir.gen_snapshot_from_sst(meta, db) {
            Ok(s) => {
                info!("gen snapshot from sst"; "meta" => ?meta);
                Ok(s)
            }
            Err(e) => {
                error!("gen snapshot from sst failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    // Downloads an SST file from an external storage.
    //
    // This method is blocking. It performs the following transformations before
    // writing to disk:
    //
    //  1. only KV pairs in the *inclusive* range (`[start, end]`) are used.
    //     (set the range to `["", ""]` to import everything).
    //  2. keys are rewritten according to the given rewrite rule.
    //
    // Both the range and rewrite keys are specified using origin keys. However,
    // the SST itself should be data keys (contain the `z` prefix). The range
    // should be specified using keys after rewriting, to be consistent with the
    // region info in PD.
    //
    // This method returns the *inclusive* key range (`[start, end]`) of SST
    // file created, or returns None if the SST is empty.
    pub fn download(
        &self,
        meta: &SSTMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        speed_limiter: Limiter,
        sst_writer: SstWriter,
    ) -> Result<Option<Range>> {
        debug!("download start";
            "meta" => ?meta,
            "url" => %(url_of_backend(backend)),
            "name" => name,
            "rewrite_rule" => ?rewrite_rule,
            "speed_limit" => speed_limiter.speed_limit(),
        );
        match self.do_download(meta, backend, name, rewrite_rule, speed_limiter, sst_writer) {
            Ok(r) => {
                info!("download"; "meta" => ?meta, "range" => ?r);
                Ok(r)
            }
            Err(e) => {
                error!("download failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    fn do_download(
        &self,
        meta: &SSTMeta,
        backend: &StorageBackend,
        name: &str,
        rewrite_rule: &RewriteRule,
        speed_limiter: Limiter,
        mut sst_writer: SstWriter,
    ) -> Result<Option<Range>> {
        let start = Instant::now();
        let path = self.dir.join(meta)?;
        let url = url_of_backend(backend);

        // prepare to download the file from the external_storage
        let ext_storage = create_storage(backend)?;
        let ext_reader = ext_storage.read(name);
        let ext_reader = speed_limiter.limit(ext_reader);

        // do the I/O copy from external_storage to the local file.
        {
            let mut file_writer = AllowStdIo::new(File::create(&path.temp)?);
            let file_length =
                block_on_external_io(copy(ext_reader, &mut file_writer)).map_err(|e| {
                    Error::CannotReadExternalStorage(url.to_string(), name.to_owned(), e)
                })?;
            if meta.length != 0 && meta.length != file_length {
                let reason = format!("length {}, expect {}", file_length, meta.length);
                return Err(Error::FileCorrupted(path.temp, reason));
            }
            IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);
            file_writer.into_inner().sync_data()?;
        }

        // now validate the SST file.
        let path_str = path.temp.to_str().unwrap();
        let sst_reader = SstReader::open(path_str)?;
        sst_reader.verify_checksum()?;

        debug!("downloaded file and verified";
            "meta" => ?meta,
            "url" => %url,
            "name" => name,
            "path" => path_str,
        );

        // undo key rewrite so we could compare with the keys inside SST
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();

        let range_start = meta.get_range().get_start();
        let range_end = meta.get_range().get_end();

        let range_start = keys::rewrite::rewrite_prefix_of_start_bound(
            new_prefix,
            old_prefix,
            key_to_bound(range_start),
        )
        .map_err(|_| {
            Error::WrongKeyPrefix("SST start range", range_start.to_vec(), new_prefix.to_vec())
        })?;
        let range_end = keys::rewrite::rewrite_prefix_of_end_bound(
            new_prefix,
            old_prefix,
            key_to_bound(range_end),
        )
        .map_err(|_| {
            Error::WrongKeyPrefix("SST end range", range_end.to_vec(), new_prefix.to_vec())
        })?;

        // read and first and last keys from the SST, determine if we could
        // simply move the entire SST instead of iterating and generate a new one.
        let mut iter = sst_reader.iter();
        let direct_retval = (|| {
            if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix
                || rewrite_rule.new_timestamp != 0
            {
                // must iterate if we perform key rewrite
                return None;
            }
            if !iter.seek(SeekKey::Start).unwrap() {
                // the SST is empty, so no need to iterate at all (should be impossible?)
                return Some(meta.get_range().clone());
            }
            let start_key = keys::origin_key(iter.key());
            if is_before_start_bound(start_key, &range_start) {
                // SST's start is before the range to consume, so needs to iterate to skip over
                return None;
            }
            let start_key = start_key.to_vec();

            // seek to end and fetch the last (inclusive) key of the SST.
            iter.seek(SeekKey::End).unwrap();
            let last_key = keys::origin_key(iter.key());
            if is_after_end_bound(last_key, &range_end) {
                // SST's end is after the range to consume
                return None;
            }

            // range contained the entire SST, no need to iterate, just moving the file is ok
            let mut range = Range::default();
            range.set_start(start_key);
            range.set_end(last_key.to_vec());
            Some(range)
        })();

        if let Some(range) = direct_retval {
            // TODO: what about encrypted SSTs?
            fs::rename(&path.temp, &path.save)?;
            let duration = start.elapsed();
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["rename"])
                .observe(duration.as_secs_f64());
            return Ok(Some(range));
        }

        // perform iteration and key rewrite.
        let mut key = keys::data_key(new_prefix);
        let new_prefix_data_key_len = key.len();
        let mut first_key = None;

        match range_start {
            Bound::Unbounded => iter.seek(SeekKey::Start)?,
            Bound::Included(s) => iter.seek(SeekKey::Key(&keys::data_key(&s)))?,
            Bound::Excluded(_) => unreachable!(),
        };
        while iter.valid().unwrap() {
            let old_key = keys::origin_key(iter.key());
            if is_after_end_bound(&old_key, &range_end) {
                break;
            }
            if !old_key.starts_with(old_prefix) {
                return Err(Error::WrongKeyPrefix(
                    "Key in SST",
                    keys::origin_key(iter.key()).to_vec(),
                    old_prefix.to_vec(),
                ));
            }
            key.truncate(new_prefix_data_key_len);
            key.extend_from_slice(&old_key[old_prefix.len()..]);
            let mut value = Cow::Borrowed(iter.value());

            if rewrite_rule.new_timestamp != 0 {
                key = Key::from_encoded(key)
                    .truncate_ts()
                    .map_err(|e| Error::Codec(e))?
                    .append_ts(rewrite_rule.new_timestamp)
                    .into_encoded();
                if meta.get_cf_name() == CF_WRITE {
                    let mut write = Write::parse(iter.value()).map_err(|e| Error::MvccError(e))?;
                    write.start_ts = rewrite_rule.new_timestamp;
                    value = Cow::Owned(write.to_bytes());
                }
            }

            sst_writer.put(&key, &value)?;
            iter.next()?;
            if first_key.is_none() {
                first_key = Some(keys::origin_key(&key).to_vec());
            }
        }

        let _ = fs::remove_file(&path.temp);

        let duration = start.elapsed();
        IMPORTER_DOWNLOAD_DURATION
            .with_label_values(&["rewrite"])
            .observe(duration.as_secs_f64());

        if let Some(start_key) = first_key {
            sst_writer.finish()?;
            let mut final_range = Range::default();
            final_range.set_start(start_key);
            final_range.set_end(keys::origin_key(&key).to_vec());
            Ok(Some(final_range))
        } else {
            // nothing is written: prevents finishing the SST at all.
            Ok(None)
        }
    }

    pub fn list_ssts(&self) -> Result<Vec<SSTMeta>> {
        self.dir.list_ssts()
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
///
/// TODO: Add size and rate limit.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";

    fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        let clone_dir = root_dir.join(Self::CLONE_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            fs::remove_dir_all(&clone_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        fs::create_dir_all(&clone_dir)?;
        Ok(ImportDir {
            root_dir,
            temp_dir,
            clone_dir,
        })
    }

    fn join(&self, meta: &SSTMeta) -> Result<ImportPath> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root_dir.join(&file_name);
        let temp_path = self.temp_dir.join(&file_name);
        let clone_path = self.clone_dir.join(&file_name);
        Ok(ImportPath {
            save: save_path,
            temp: temp_path,
            clone: clone_path,
        })
    }

    fn create(&self, meta: &SSTMeta) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        ImportFile::create(meta.clone(), path)
    }

    fn delete(&self, meta: &SSTMeta) -> Result<ImportPath> {
        let path = self.join(meta)?;
        if path.save.exists() {
            fs::remove_file(&path.save)?;
        }
        if path.temp.exists() {
            fs::remove_file(&path.temp)?;
        }
        if path.clone.exists() {
            fs::remove_file(&path.clone)?;
        }
        Ok(path)
    }

    fn pre_ingest(&self, meta: &SSTMeta, db: &DB) -> Result<ImportPath> {
        let path = self.join(meta)?;
        let cf = meta.get_cf_name();
        prepare_sst_for_ingestion(&path.save, &path.clone)?;
        let length = meta.get_length();
        let crc32 = meta.get_crc32();
        // FIXME perform validate_sst_for_ingestion after we can handle sst file size correctly.
        // currently we can not handle sst file size after rewrite,
        // we need re-compute length & crc32 and fill back to sstMeta.
        if length != 0 && crc32 != 0 {
            // we only validate if the length and CRC32 are explicitly provided.
            validate_sst_for_ingestion(db, cf, &path.clone, length, crc32)?;
            IMPORTER_INGEST_BYTES.observe(length as _)
        } else {
            debug!("skipping SST validation since length and crc32 are both 0");
        }
        Ok(path)
    }

    fn ingest(&self, meta: &SSTMeta, db: &DB) -> Result<()> {
        let start = Instant::now();
        let path = self.pre_ingest(meta, db)?;

        let handle = get_cf_handle(db, meta.get_cf_name())?;
        let mut opts = IngestExternalFileOptions::new();
        opts.move_files(true);
        db.ingest_external_file_cf(handle, &opts, &[path.clone.to_str().unwrap()])?;
        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(start.elapsed().as_secs_f64());
        Ok(())
    }

    fn gen_snapshot_from_sst(&self, meta: &SSTMeta, db: &DB) -> Result<invoke::SnapshotKV> {
        let start = Instant::now();
        let path = self.pre_ingest(meta, db)?;

        let snap = gen_snap_kv_data_from_sst(path.clone.to_str().unwrap());

        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(start.elapsed().as_secs_f64());

        Ok(snap)
    }

    fn list_ssts(&self) -> Result<Vec<SSTMeta>> {
        let mut ssts = Vec::new();
        for e in fs::read_dir(&self.root_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() {
                continue;
            }
            let path = e.path();
            match path_to_sst_meta(&path) {
                Ok(sst) => ssts.push(sst),
                Err(e) => {
                    error!("path_to_sst_meta failed"; "path" => %path.to_str().unwrap(), "err" => %e);
                }
            }
        }
        Ok(ssts)
    }
}

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    save: PathBuf,
    // The path of the file that is being uploaded.
    temp: PathBuf,
    // The path of the file that is going to be ingested.
    clone: PathBuf,
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SSTMeta,
    path: ImportPath,
    file: Option<File>,
    digest: crc32fast::Hasher,
}

impl ImportFile {
    fn create(meta: SSTMeta, path: ImportPath) -> Result<ImportFile> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path.temp)?;
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32fast::Hasher::new(),
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.update(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.file.take().unwrap().sync_all()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            fs::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.clone().finalize();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }

        let f = self.file.as_ref().unwrap();
        let length = f.metadata()?.len();
        let expect = self.meta.get_length();
        if length != expect {
            let reason = format!("length {}, expect {}", length, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

const SST_SUFFIX: &str = ".sst";

fn sst_meta_to_path(meta: &SSTMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}{}",
        Uuid::from_bytes(meta.get_uuid())?,
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
        SST_SUFFIX,
    )))
}

fn path_to_sst_meta<P: AsRef<Path>>(path: P) -> Result<SSTMeta> {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return Err(Error::InvalidSSTPath(path.to_owned())),
    };

    // A valid file name should be in the format:
    // "{uuid}_{region_id}_{region_epoch.conf_ver}_{region_epoch.version}.sst"
    if !file_name.ends_with(SST_SUFFIX) {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }
    let elems: Vec<_> = file_name.trim_end_matches(SST_SUFFIX).split('_').collect();
    if elems.len() != 4 {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }

    let mut meta = SSTMeta::new();
    let uuid = Uuid::parse_str(elems[0])?;
    meta.set_uuid(uuid.as_bytes().to_vec());
    meta.set_region_id(elems[1].parse()?);
    meta.mut_region_epoch().set_conf_ver(elems[2].parse()?);
    meta.mut_region_epoch().set_version(elems[3].parse()?);
    Ok(meta)
}

fn key_to_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(key)
    }
}

fn is_before_start_bound(value: &[u8], bound: &Bound<Vec<u8>>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value < **b,
        Bound::Excluded(b) => *value <= **b,
    }
}

fn is_after_end_bound(value: &[u8], bound: &Bound<Vec<u8>>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value > **b,
        Bound::Excluded(b) => *value >= **b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::test_helpers::*;

    use std::collections::HashMap;
    use std::f64::INFINITY;
    use std::sync::Arc;

    use crate::storage::mvcc::WriteType;
    use crate::storage::Value;
    use engine::rocks::util::{new_engine, CFOptions};
    use engine::rocks::{
        ColumnFamilyOptions, DBEntryType, SstWriterBuilder, TablePropertiesCollector,
        TablePropertiesCollectorFactory,
    };
    use engine::util::get_range_properties_cf;
    use engine::{name_to_cf, CF_DEFAULT, CF_WRITE, DATA_CFS};
    use tempdir::TempDir;
    use tikv_util::file::calc_crc32_bytes;

    const PROP_TEST_MARKER_CF_NAME: &[u8] = b"tikv.test_marker_cf_name";

    #[derive(Default)]
    struct TestPropertiesCollectorFactory {
        cf: String,
    }

    impl TestPropertiesCollectorFactory {
        pub fn new(cf: impl Into<String>) -> Self {
            Self { cf: cf.into() }
        }
    }

    impl TablePropertiesCollectorFactory for TestPropertiesCollectorFactory {
        fn create_table_properties_collector(
            &mut self,
            _: u32,
        ) -> Box<dyn TablePropertiesCollector> {
            Box::new(TestPropertiesCollector::new(self.cf.clone()))
        }
    }

    struct TestPropertiesCollector {
        cf: String,
    }

    impl TestPropertiesCollector {
        pub fn new(cf: String) -> Self {
            Self { cf }
        }
    }

    impl TablePropertiesCollector for TestPropertiesCollector {
        fn add(&mut self, _: &[u8], _: &[u8], _: DBEntryType, _: u64, _: u64) {}

        fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
            std::iter::once((
                PROP_TEST_MARKER_CF_NAME.to_owned(),
                self.cf.as_bytes().to_owned(),
            ))
            .collect()
        }
    }

    fn new_engine_with_properties(path: &str, cfs: &[&str]) -> DB {
        let cf_opts = cfs
            .iter()
            .map(|cf| {
                let mut opt = ColumnFamilyOptions::new();
                opt.add_table_properties_collector_factory(
                    "tikv.test_properties",
                    Box::new(TestPropertiesCollectorFactory::new(*cf)),
                );
                CFOptions::new(*cf, opt)
            })
            .collect();
        new_engine(path, None, cfs, Some(cf_opts)).expect("rocks test engine")
    }

    #[test]
    fn test_import_dir() {
        let temp_dir = TempDir::new("test_import_dir").unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SSTMeta::new();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
            // Cannot create the same file again.
            assert!(dir.create(&meta).is_err());
        }

        // Test ImportDir::delete()
        {
            File::create(&path.temp).unwrap();
            File::create(&path.save).unwrap();
            File::create(&path.clone).unwrap();
            dir.delete(&meta).unwrap();
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
        }

        // Test ImportDir::ingest()

        let db_path = temp_dir.path().join("db");
        let db = new_engine(db_path.to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap();

        let cases = vec![(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &range) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(&path, range);

            let mut f = dir.create(&meta).unwrap();
            f.append(&data).unwrap();
            f.finish().unwrap();

            dir.ingest(&meta, &db).unwrap();
            check_db_range(&db, range);

            ingested.push(meta);
        }

        let ssts = dir.list_ssts().unwrap();
        assert_eq!(ssts.len(), ingested.len());
        for sst in &ssts {
            ingested
                .iter()
                .find(|s| s.get_uuid() == sst.get_uuid())
                .unwrap();
            dir.delete(sst).unwrap();
        }
        assert!(dir.list_ssts().unwrap().is_empty());
    }

    #[test]
    fn test_import_file() {
        let temp_dir = TempDir::new("test_import_file").unwrap();

        let path = ImportPath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
            clone: temp_dir.path().join("clone"),
        };

        let data = b"test_data";
        let crc32 = calc_crc32_bytes(data);

        let mut meta = SSTMeta::new();

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            // Cannot create the same file again.
            assert!(ImportFile::create(meta.clone(), path.clone()).is_err());
            f.append(data).unwrap();
            // Invalid crc32 and length.
            assert!(f.finish().is_err());
            assert!(path.temp.exists());
            assert!(!path.save.exists());
        }

        meta.set_crc32(crc32);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            // Invalid length.
            assert!(f.finish().is_err());
        }

        meta.set_length(data.len() as u64);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            f.finish().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
        }
    }

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SSTMeta::new();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.mut_region_epoch().set_conf_ver(2);
        meta.mut_region_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);

        let new_meta = path_to_sst_meta(path).unwrap();
        assert_eq!(meta, new_meta);
    }

    fn create_sample_external_sst_file() -> Result<(TempDir, StorageBackend, SSTMeta)> {
        let ext_sst_dir = TempDir::new("external_storage")?;
        let mut sst_writer = SstWriterBuilder::new()
            .build(ext_sst_dir.path().join("sample.sst").to_str().unwrap())?;
        sst_writer.put(b"zt123_r01", b"abc")?;
        sst_writer.put(b"zt123_r04", b"xyz")?;
        sst_writer.put(b"zt123_r07", b"pqrst")?;
        // sst_writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
        sst_writer.put(b"zt123_r13", b"www")?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SSTMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn get_encoded_key(key: &[u8], ts: u64) -> Vec<u8> {
        keys::data_key(Key::from_raw(key).append_ts(ts).as_encoded())
    }

    fn get_write_value(
        write_type: WriteType,
        start_ts: u64,
        short_value: Option<Value>,
    ) -> Vec<u8> {
        Write::new(write_type, start_ts, short_value).to_bytes()
    }

    fn create_sample_external_sst_file_txn_default() -> Result<(TempDir, StorageBackend, SSTMeta)> {
        let ext_sst_dir = TempDir::new("external_storage")?;
        let mut sst_writer = SstWriterBuilder::new().build(
            ext_sst_dir
                .path()
                .join("sample_default.sst")
                .to_str()
                .unwrap(),
        )?;
        sst_writer.put(&get_encoded_key(b"t123_r01", 1), b"abc")?;
        sst_writer.put(&get_encoded_key(b"t123_r04", 3), b"xyz")?;
        sst_writer.put(&get_encoded_key(b"t123_r07", 7), b"pqrst")?;
        // sst_writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SSTMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn create_sample_external_sst_file_txn_write() -> Result<(TempDir, StorageBackend, SSTMeta)> {
        let ext_sst_dir = TempDir::new("external_storage")?;
        let mut sst_writer = SstWriterBuilder::new().build(
            ext_sst_dir
                .path()
                .join("sample_write.sst")
                .to_str()
                .unwrap(),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r01", 5),
            &get_write_value(WriteType::Put, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r02", 5),
            &get_write_value(WriteType::Delete, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r04", 4),
            &get_write_value(WriteType::Put, 3, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r07", 8),
            &get_write_value(WriteType::Put, 7, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r13", 8),
            &get_write_value(WriteType::Put, 7, Some(b"www".to_vec())),
        )?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SSTMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name(CF_WRITE.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        Ok((ext_sst_dir, backend, meta))
    }

    fn new_rewrite_rule(
        old_key_prefix: &[u8],
        new_key_prefix: &[u8],
        new_timestamp: u64,
    ) -> RewriteRule {
        let mut rule = RewriteRule::new();
        rule.set_old_key_prefix(old_key_prefix.to_vec());
        rule.set_new_key_prefix(new_key_prefix.to_vec());
        rule.set_new_timestamp(new_timestamp);
        rule
    }

    fn create_sst_writer_with_db(importer: &SSTImporter, meta: &SSTMeta) -> Result<SstWriter> {
        let temp_dir = TempDir::new("test_import_dir").unwrap();
        let db_path = temp_dir.path().join("db");
        let db = new_engine_with_properties(db_path.to_str().unwrap(), DATA_CFS);

        let sst_writer = SstWriterBuilder::new()
            .set_db(Arc::new(db))
            .set_cf(name_to_cf(meta.get_cf_name()).unwrap())
            .build(importer.get_path(meta).to_str().unwrap())
            .unwrap();
        Ok(sst_writer)
    }

    #[test]
    fn test_download_sst_no_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        dbg!(&range);

        assert_eq!(range.get_start(), b"t123_r01");
        assert_eq!(range.get_end(), b"t123_r13");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (b"zt123_r01".to_vec(), b"abc".to_vec()),
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt123_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t567", 0),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t567_r01");
        assert_eq!(range.get_end(), b"t567_r13");

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (b"zt567_r01".to_vec(), b"abc".to_vec()),
                (b"zt567_r04".to_vec(), b"xyz".to_vec()),
                (b"zt567_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt567_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_default() {
        // performs the download.
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_default().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        let _ = importer
            .download(
                &meta,
                &backend,
                "sample_default.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (get_encoded_key(b"t123_r01", 16), b"abc".to_vec()),
                (get_encoded_key(b"t123_r04", 16), b"xyz".to_vec()),
                (get_encoded_key(b"t123_r07", 16), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_write() {
        // performs the download.
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file_txn_write().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        let _ = importer
            .download(
                &meta,
                &backend,
                "sample_write.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (
                    get_encoded_key(b"t123_r01", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r02", 16),
                    get_write_value(WriteType::Delete, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r04", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r07", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r13", 16),
                    get_write_value(WriteType::Put, 16, Some(b"www".to_vec()))
                ),
            ]
        );
    }

    #[test]
    fn test_download_sst_then_ingest() {
        for cf in &[CF_DEFAULT, CF_WRITE] {
            // creates a sample SST file.
            let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
            meta.set_cf_name(cf.to_string());

            // performs the download.
            let importer_dir = TempDir::new("importer_dir").unwrap();
            let importer = SSTImporter::new(importer_dir.path()).unwrap();
            let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

            let range = importer
                .download(
                    &meta,
                    &backend,
                    "sample.sst",
                    &new_rewrite_rule(b"t123", b"t9102", 0),
                    Limiter::new(INFINITY),
                    sst_writer,
                )
                .unwrap()
                .unwrap();

            assert_eq!(range.get_start(), b"t9102_r01");
            assert_eq!(range.get_end(), b"t9102_r13");

            // performs the ingest
            let ingest_dir = TempDir::new("ingest_dir").unwrap();
            let db = new_engine_with_properties(ingest_dir.path().to_str().unwrap(), DATA_CFS);

            meta.set_length(0); // disable validation.
            meta.set_crc32(0);

            {
                let snap = importer.gen_snapshot_from_sst(&meta, &db).unwrap();
                let v = snap.clone().into_iter().collect::<Vec<_>>();
                assert_eq!(
                    v,
                    vec![
                        (b"t9102_r01".to_vec(), b"abc".to_vec()),
                        (b"t9102_r04".to_vec(), b"xyz".to_vec()),
                        (b"t9102_r07".to_vec(), b"pqrst".to_vec()),
                        (b"t9102_r13".to_vec(), b"www".to_vec()),
                    ]
                );
            }

            importer.ingest(&meta, &db).unwrap();

            // verifies the DB content is correct.
            let cf_handle = get_cf_handle(&db, cf).unwrap();
            let mut iter = db.iter_cf(cf_handle);
            iter.seek(SeekKey::Start).unwrap();
            assert_eq!(
                iter.collect::<Vec<_>>(),
                vec![
                    (b"zt9102_r01".to_vec(), b"abc".to_vec()),
                    (b"zt9102_r04".to_vec(), b"xyz".to_vec()),
                    (b"zt9102_r07".to_vec(), b"pqrst".to_vec()),
                    (b"zt9102_r13".to_vec(), b"www".to_vec()),
                ]
            );

            // check properties
            let start = keys::data_key(b"");
            let end = keys::data_end_key(b"");
            let collection = get_range_properties_cf(&db, cf, &start, &end).unwrap();
            assert!(!collection.is_empty());
            for (_, v) in &*collection {
                assert!(!v.user_collected_properties().is_empty());
                assert_eq!(
                    v.user_collected_properties()
                        .get(PROP_TEST_MARKER_CF_NAME)
                        .unwrap(),
                    cf.as_bytes()
                );
            }
        }
    }

    #[test]
    fn test_download_sst_partial_range() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        // note: the range doesn't contain the DATA_PREFIX 'z'.
        meta.mut_range().set_start(b"t123_r02".to_vec());
        meta.mut_range().set_end(b"t123_r12".to_vec());
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t123_r04");
        assert_eq!(range.get_end(), b"t123_r07");

        // verifies that the file is saved to the correct place.
        // (the file size is changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_partial_range_with_key_rewrite() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        meta.mut_range().set_start(b"t5_r02".to_vec());
        meta.mut_range().set_end(b"t5_r12".to_vec());
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let range = importer
            .download(
                &meta,
                &backend,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t5", 0),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(range.get_start(), b"t5_r04");
        assert_eq!(range.get_end(), b"t5_r07");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                (b"zt5_r04".to_vec(), b"xyz".to_vec()),
                (b"zt5_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_invalid() {
        let ext_sst_dir = TempDir::new("external_storage").unwrap();
        fs::write(ext_sst_dir.path().join("sample.sst"), b"not an SST file").unwrap();

        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        let backend = external_storage::make_local_backend(ext_sst_dir.path());
        let mut meta = SSTMeta::new();
        meta.set_uuid(vec![0u8; 16]);
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
        );
        match &result {
            Err(Error::RocksDB(msg)) if msg.starts_with("Corruption:") => {}
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_empty() {
        let (_ext_sst_dir, backend, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();

        meta.mut_range().set_start(vec![b'x']);
        meta.mut_range().set_end(vec![b'y']);
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
        );

        match result {
            Ok(None) => {}
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_wrong_key_prefix() {
        let (_ext_sst_dir, backend, meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = TempDir::new("importer_dir").unwrap();
        let importer = SSTImporter::new(importer_dir.path()).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let result = importer.download(
            &meta,
            &backend,
            "sample.sst",
            &new_rewrite_rule(b"xxx", b"yyy", 0),
            Limiter::new(INFINITY),
            sst_writer,
        );

        match &result {
            Err(Error::WrongKeyPrefix(_, key, prefix)) => {
                assert_eq!(key, b"t123_r01");
                assert_eq!(prefix, b"xxx");
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }
}
