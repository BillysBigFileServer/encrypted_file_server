pub use crate::crypto::*;

pub use crate::bfsp::files::*;
use anyhow::{anyhow, Error, Result};
pub use prost::Message;
use sqlx::{sqlite::SqliteRow, Row, Sqlite};
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};
use uuid::Uuid;

impl FileServerMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec().prepend_len()
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::decode(bytes).map_err(|e| anyhow!("{e:?}"))
    }
}

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct ChunkID {
    pub id: u128,
}

impl Debug for ChunkID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:#x}", self.id))
    }
}

impl ChunkID {
    pub fn to_bytes(&self) -> [u8; 16] {
        self.id.to_le_bytes()
    }
    pub fn from_bytes(buf: [u8; 16]) -> Self {
        Self {
            id: u128::from_le_bytes(buf),
        }
    }
}

impl sqlx::Type<Sqlite> for ChunkID {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
}

impl std::fmt::Display for ChunkID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let uuid = Uuid::from_u128(self.id);
        f.write_str(&uuid.to_string())
    }
}

impl sqlx::FromRow<'_, SqliteRow> for ChunkID {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        row.try_get::<String, &str>("id")
            .map(|chunk_id: String| Self {
                id: Uuid::from_str(&chunk_id).unwrap().as_u128(),
            })
    }
}

impl sqlx::Encode<'_, Sqlite> for ChunkID {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        buf.push(sqlx::sqlite::SqliteArgumentValue::Text(
            Uuid::from_u128(self.id).to_string().into(),
        ));

        sqlx::encode::IsNull::No
    }
}

impl TryFrom<Vec<u8>> for ChunkID {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let uuid_bytes: [u8; 16] = value.try_into().map_err(|e| anyhow!("{e:?}"))?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        Ok(ChunkID { id: uuid.as_u128() })
    }
}

impl TryFrom<String> for ChunkID {
    type Error = Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Ok(ChunkID {
            id: Uuid::from_str(&value)?.as_u128(),
        })
    }
}

impl ChunkID {
    /// Uses the chunk has as an RNG, FIXME insecure as shit
    /// This reduces the number of unknown bits in the file hash by HALF, which reduces the anonimity of any files being uploaded
    pub fn new(hash: &ChunkHash) -> Self {
        let bytes: [u8; blake3::OUT_LEN] = *hash.0.as_bytes();
        let uuid_bytes: [u8; 16] = bytes[..16].try_into().unwrap();
        let uuid = Uuid::from_bytes(uuid_bytes);

        Self { id: uuid.as_u128() }
    }
}

// TODO: encrypt chunk metadata that isn't ID??
impl ChunkMetadata {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = self.encode_to_vec();
        let mut msg = (buf.len() as u32).to_le_bytes().to_vec();
        msg.append(&mut buf);
        msg
    }
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        Self::decode(buf).map_err(|e| anyhow!("{e:?}"))
    }
}

#[derive(PartialEq)]
pub struct ChunkHash(blake3::Hash);

impl ChunkHash {
    pub fn to_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    pub fn from_bytes(buf: [u8; blake3::OUT_LEN]) -> Self {
        Self(blake3::Hash::from_bytes(buf))
    }
}

impl From<blake3::Hash> for ChunkHash {
    fn from(value: blake3::Hash) -> Self {
        Self(value)
    }
}

impl sqlx::Type<Sqlite> for ChunkHash {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
}

impl sqlx::FromRow<'_, SqliteRow> for ChunkHash {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        row.try_get::<String, &str>("hash")
            .map(|hash: String| Self(blake3::Hash::from_str(&hash).unwrap()))
    }
}

impl sqlx::Encode<'_, Sqlite> for ChunkHash {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        buf.push(sqlx::sqlite::SqliteArgumentValue::Text(
            self.to_string().into(),
        ));
        sqlx::encode::IsNull::No
    }
}

impl ToString for ChunkHash {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl TryFrom<&[u8]> for ChunkHash {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> anyhow::Result<Self> {
        let slice: &[u8; blake3::OUT_LEN] = value.try_into()?;
        Ok(Self(blake3::Hash::from_bytes(*slice)))
    }
}

impl TryFrom<Vec<u8>> for ChunkHash {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> anyhow::Result<Self> {
        value.as_slice().try_into()
    }
}

impl Into<[u8; blake3::OUT_LEN]> for ChunkHash {
    fn into(self) -> [u8; blake3::OUT_LEN] {
        *self.0.as_bytes()
    }
}

impl TryFrom<String> for ChunkHash {
    type Error = anyhow::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Ok(Self(blake3::Hash::from_str(&value)?))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileHash(pub(crate) blake3::Hash);

impl std::fmt::Display for FileHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl sqlx::FromRow<'_, SqliteRow> for FileHash {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        row.try_get::<String, &str>("file_hash")
            .map(|hash: String| Self(blake3::Hash::from_str(&hash).unwrap()))
    }
}

impl sqlx::Type<Sqlite> for FileHash {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
}

impl sqlx::Encode<'_, Sqlite> for FileHash {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        buf.push(sqlx::sqlite::SqliteArgumentValue::Text(
            self.0.to_string().into(),
        ));

        sqlx::encode::IsNull::No
    }
}

impl From<blake3::Hash> for FileHash {
    fn from(value: blake3::Hash) -> Self {
        Self(value)
    }
}

impl TryFrom<String> for FileHash {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Ok(Self(blake3::Hash::from_str(&value)?))
    }
}

impl TryFrom<&[u8]> for FileHash {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> anyhow::Result<Self> {
        let slice: &[u8; blake3::OUT_LEN] = value.try_into().map_err(|_err| {
            anyhow!(
                "Could not convert slice of length {} to [u8; 32]",
                value.len()
            )
        })?;
        Ok(Self(blake3::Hash::from_bytes(*slice)))
    }
}

impl TryFrom<Vec<u8>> for FileHash {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> anyhow::Result<Self> {
        value.as_slice().try_into()
    }
}

#[derive(Debug)]
pub struct AuthErr;

impl Display for AuthErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authenticaiton error")
    }
}

impl std::error::Error for AuthErr {}

#[derive(Debug)]
pub struct ChunkNotFound;

impl Display for ChunkNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Chunk not found")
    }
}

impl std::error::Error for ChunkNotFound {}

pub trait PrependLen {
    fn prepend_len(self) -> Self;
}
impl PrependLen for Vec<u8> {
    fn prepend_len(mut self) -> Self {
        let len = self.len();
        let mut len_bytes = (len as u32).to_le_bytes().to_vec();
        len_bytes.append(&mut self);
        len_bytes
    }
}
