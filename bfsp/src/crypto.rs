use anyhow::{anyhow, Result};
use blake3::Hasher;
use chacha20poly1305::{aead::OsRng, AeadInPlace, Key, KeyInit, XChaCha20Poly1305};
use rkyv::{Archive, Deserialize, Serialize};
use sqlx::Sqlite;
use tokio::{fs::File, io::AsyncReadExt};

use crate::{ChunkHash, ChunkMetadata, FileHash};

pub struct EncryptionKey {
    key: Key,
}

impl TryFrom<Vec<u8>> for EncryptionKey {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let mut key: Key = [0; 32].into();
        key.copy_from_slice(&value);

        Ok(Self { key })
    }
}

impl sqlx::Type<Sqlite> for EncryptionKey {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <&[u8] as sqlx::Type<Sqlite>>::type_info()
    }
}

impl sqlx::Encode<'_, Sqlite> for EncryptionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        let nonce = self.key.to_vec().into();
        buf.push(sqlx::sqlite::SqliteArgumentValue::Blob(nonce));

        sqlx::encode::IsNull::No
    }
}

impl EncryptionKey {
    pub fn new() -> Self {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Self { key }
    }

    pub fn encrypt_chunk_in_place(
        &self,
        chunk: &mut Vec<u8>,
        chunk_meta: &ChunkMetadata,
    ) -> Result<()> {
        let key = XChaCha20Poly1305::new(&self.key);
        key.encrypt_in_place(
            chunk_meta.nonce.nonce.as_slice().into(),
            chunk_meta.id.to_bytes().as_slice(),
            chunk,
        )?;

        Ok(())
    }
    pub fn decrypt_chunk_in_place(
        &self,
        chunk: &mut Vec<u8>,
        chunk_meta: &ChunkMetadata,
    ) -> Result<()> {
        let key = XChaCha20Poly1305::new(&self.key);
        key.decrypt_in_place(
            chunk_meta.nonce.nonce.as_slice().into(),
            chunk_meta.id.to_bytes().as_slice(),
            chunk,
        )?;
        *chunk = zstd::bulk::decompress(&chunk, chunk_meta.size as usize)?;
        Ok(())
    }
}

#[derive(Clone, sqlx::FromRow, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq), check_bytes)]
pub struct EncryptionNonce {
    nonce: [u8; 24],
}

impl sqlx::Type<Sqlite> for EncryptionNonce {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <&[u8] as sqlx::Type<Sqlite>>::type_info()
    }
}

impl sqlx::Encode<'_, Sqlite> for EncryptionNonce {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        let nonce = self.nonce.to_vec().into();
        buf.push(sqlx::sqlite::SqliteArgumentValue::Blob(nonce));

        sqlx::encode::IsNull::No
    }
}

impl TryFrom<Vec<u8>> for EncryptionNonce {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        Ok(Self {
            nonce: value.try_into().map_err(|e| anyhow!("{e:?}"))?,
        })
    }
}

impl EncryptionNonce {
    pub fn new(chunk_hash: &ChunkHash) -> Self {
        let mut nonce: [u8; 24] = [0; 24];
        nonce.copy_from_slice(&chunk_hash.to_bytes()[0..24]);

        Self { nonce }
    }
}

pub async fn hash_file(file: &mut File) -> Result<FileHash> {
    let chunk_size = 8388608 * 8;

    let mut total_file_hasher = Hasher::new();
    let mut chunk_buf = vec![0; chunk_size];
    let mut chunk_buf_index = 0;

    loop {
        // First, read into the buffer until it's full, or we hit an EOF
        let eof = loop {
            if chunk_buf_index == chunk_buf.len() {
                break false;
            }
            match file.read(&mut chunk_buf[chunk_buf_index..]).await {
                Ok(num_bytes_read) => match num_bytes_read {
                    0 => break true,
                    b => chunk_buf_index += b,
                },
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => break true,
                    _ => return anyhow::Result::Err(err.into()),
                },
            };
        };

        let chunk_buf = &chunk_buf[..chunk_buf_index];

        total_file_hasher.update_rayon(chunk_buf);

        if eof {
            break;
        }

        chunk_buf_index = 0;
    }

    Ok(FileHash(total_file_hasher.finalize()))
}

pub fn hash_chunk(chunk: &[u8]) -> ChunkHash {
    let mut hasher = Hasher::new();
    hasher.update(chunk);
    hasher.finalize().into()
}

pub fn parallel_hash_chunk(chunk: &[u8]) -> ChunkHash {
    let mut hasher = Hasher::new();
    hasher.update(chunk);
    hasher.finalize().into()
}
