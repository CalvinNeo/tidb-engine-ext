// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate serde_derive;

use std::{ops::Deref, sync::Arc};

use bytes::Buf;
use hmac::{Hmac, Mac, NewMac};
use openssl::{
    symm,
    symm::{Cipher, Crypter, Mode},
};
use rand::RngCore;

type Hmac256 = Hmac<sha2::Sha256>;

const KEY_TYPE_ENCRYPTED_BY_MASTER_AES_256_CTR: u8 = 1;
const KEY_EXPORTED_SIZE: usize = 1 + 4 + 64;

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
pub struct KeyspaceEncryptionConfig {
    pub enabled: bool,
}

#[derive(Clone)]
pub struct EncryptionKey {
    pub core: Arc<EncryptionKeyCore>,
}

impl EncryptionKey {
    pub fn new(cipher_text: Vec<u8>, plain_text: Vec<u8>, current_ver: u32) -> Self {
        let current_key = EncryptionKeyCore::new_current_key(&plain_text, current_ver);
        Self {
            core: Arc::new(EncryptionKeyCore {
                plain_text,
                cipher_text,
                current_ver,
                current_key,
            }),
        }
    }

    pub fn set_version(&self, key_ver: u32) -> Self {
        let core = Arc::new(self.core.set_version(key_ver));
        EncryptionKey { core }
    }
}

impl Deref for EncryptionKey {
    type Target = EncryptionKeyCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct EncryptionKeyCore {
    pub cipher_text: Vec<u8>,
    pub plain_text: Vec<u8>,
    pub current_key: Vec<u8>,
    pub current_ver: u32,
}

impl EncryptionKeyCore {
    pub fn encrypt(&self, data: &[u8], iv_high: u64, iv_low: u32, buf: &mut Vec<u8>) {
        let mut iv = [0u8; 16];
        iv[0..8].copy_from_slice(&iv_high.to_be_bytes());
        iv[8..12].copy_from_slice(&iv_low.to_be_bytes());
        let cipher = Cipher::aes_256_ctr();
        let mut crypter =
            Crypter::new(cipher, Mode::Encrypt, &self.current_key, Some(&iv)).unwrap();
        let origin_len = buf.len();
        let additional_len = data.len() + cipher.block_size();
        buf.resize(origin_len + additional_len, 0);
        let dst = &mut buf[origin_len..];
        let count = crypter.update(data, dst).unwrap();
        let rest = crypter.finalize(&mut dst[count..]).unwrap();
        buf.truncate(origin_len + count + rest);
    }

    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        iv_high: u64,
        iv_low: u32,
        key_ver: u32,
        buf: &mut Vec<u8>,
    ) {
        let mut iv = [0u8; 16];
        iv[0..8].copy_from_slice(&iv_high.to_be_bytes());
        iv[8..12].copy_from_slice(&iv_low.to_be_bytes());
        let mut versioned_key_buf = [0u8; 32];
        let versioned_key = if self.current_ver != key_ver {
            let mut hmac = Hmac256::new_varkey(&self.plain_text).unwrap();
            hmac.update(&key_ver.to_be_bytes());
            versioned_key_buf[..].copy_from_slice(hmac.finalize().into_bytes().as_slice());
            &versioned_key_buf[..]
        } else {
            &self.current_key
        };
        let cipher = Cipher::aes_256_ctr();
        let mut crypter = Crypter::new(cipher, Mode::Decrypt, versioned_key, Some(&iv)).unwrap();
        let origin_len = buf.len();
        let additional_len = ciphertext.len() + cipher.block_size();
        buf.resize(origin_len + additional_len, 0);
        let dst = &mut buf[origin_len..];
        let count = crypter.update(ciphertext, dst).unwrap();
        let rest = crypter.finalize(&mut dst[count..]).unwrap();
        buf.truncate(origin_len + count + rest);
    }

    pub fn set_version(&self, key_ver: u32) -> Self {
        let current_key = Self::new_current_key(&self.plain_text, key_ver);
        Self {
            plain_text: self.plain_text.clone(),
            cipher_text: self.cipher_text.clone(),
            current_ver: key_ver,
            current_key,
        }
    }

    pub fn export(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(KEY_EXPORTED_SIZE);
        data.push(KEY_TYPE_ENCRYPTED_BY_MASTER_AES_256_CTR);
        data.extend_from_slice(&self.current_ver.to_be_bytes());
        data.extend_from_slice(&self.cipher_text);
        data
    }

    fn new_current_key(plain_text: &[u8], key_version: u32) -> Vec<u8> {
        let mut hmac = Hmac256::new_varkey(plain_text).unwrap();
        hmac.update(&key_version.to_be_bytes());
        hmac.finalize().into_bytes().to_vec()
    }

    pub fn encryption_block_size(&self) -> usize {
        Cipher::aes_256_ctr().block_size()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MasterKeyConfig {
    pub key_id: String,
    pub cipher_text: String,
    pub region: String,
    pub vendor: String,
    pub endpoint: String,
}

impl MasterKeyConfig {
    pub fn override_from_env(&mut self) {
        Self::env_or_default("CSE_MASTER_KEY_ID", &mut self.key_id);
        Self::env_or_default("CSE_MASTER_KEY_CIPHER_TEXT", &mut self.cipher_text);
        Self::env_or_default("CSE_MASTER_KEY_VENDOR", &mut self.vendor);
        Self::env_or_default("AWS_REGION", &mut self.region);
    }

    fn env_or_default(name: &str, val: &mut String) {
        if let Ok(v) = std::env::var(name) {
            *val = v;
        }
    }
}

#[derive(Clone)]
pub struct MasterKey {
    core: Arc<MasterKeyCore>,
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "master key redacted")
    }
}

impl MasterKey {
    pub fn new(plain_text: &[u8]) -> Self {
        Self {
            core: Arc::new(MasterKeyCore {
                master_key: plain_text.to_vec(),
            }),
        }
    }
}

impl Deref for MasterKey {
    type Target = MasterKeyCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct MasterKeyCore {
    master_key: Vec<u8>,
}

impl MasterKeyCore {
    pub fn generate_encryption_key(&self) -> EncryptionKey {
        let mut rng = rand::thread_rng();
        let mut plain_text = vec![0u8; 64];
        rng.fill_bytes(&mut plain_text[..]);
        let cipher_text =
            symm::encrypt(Cipher::aes_256_ctr(), &self.master_key, None, &plain_text).unwrap();
        EncryptionKey::new(cipher_text, plain_text, 0)
    }

    pub fn decrypt_encryption_key(&self, mut exported: &[u8]) -> Result<EncryptionKey, String> {
        if exported.len() != KEY_EXPORTED_SIZE {
            return Err(format!("invalid exported key len {}", exported.len()));
        }
        let key_type = exported.get_u8();
        if key_type != KEY_TYPE_ENCRYPTED_BY_MASTER_AES_256_CTR {
            return Err(format!("invalid exported key type {}", exported[0]));
        }
        let key_ver = exported.get_u32();
        let cipher_text = exported.to_vec();
        let plain_text =
            symm::decrypt(Cipher::aes_256_ctr(), &self.master_key, None, &cipher_text).unwrap();
        Ok(EncryptionKey::new(cipher_text, plain_text, key_ver))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_encryption() {
        let master_key_plain_text = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let master_key = MasterKey::new(&master_key_plain_text);
        let encryption_key = master_key.generate_encryption_key();
        let mut encrypted = vec![];
        encryption_key.encrypt(b"hello", 123, 12, &mut encrypted);
        let mut origin = vec![];
        encryption_key.decrypt(&encrypted, 123, 12, 0, &mut origin);

        let encryption_key_v2 = encryption_key.set_version(2);
        let mut encrypted_v2 = vec![];
        encryption_key_v2.encrypt(b"hello", 123, 12, &mut encrypted_v2);
        let mut origin = vec![];
        encryption_key_v2.decrypt(&encrypted, 123, 12, 0, &mut origin);
        assert_eq!(origin, b"hello");
        origin.truncate(0);
        encryption_key.decrypt(&encrypted_v2, 123, 12, 2, &mut origin);
        assert_eq!(origin, b"hello");
    }
}
