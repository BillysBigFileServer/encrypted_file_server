use crate::auth::Authentication;
pub use crate::crypto::*;

use anyhow::{anyhow, Error, Result};
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, Sqlite};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    str::FromStr,
};
use uuid::Uuid;

#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct FileServerMessage {
    pub auth: Authentication,
    pub action: Action,
}

impl FileServerMessage {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        debug_assert!({
            let msg = rkyv::check_archived_root::<FileServerMessage>(&bytes).unwrap();
            let msg: FileServerMessage = msg.deserialize(&mut rkyv::Infallible).unwrap();

            msg.auth.macaroon == self.auth.macaroon
        });

        println!("ChunkMetadata is {}", bytes.len());
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(4);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}

//TODO: maybe include all the data being sent in the enum itself?
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum Action {
    UploadChunk {
        chunk_metadata: ChunkMetadata,
        chunk: Vec<u8>,
    },
    QueryChunksUploaded {
        chunks: HashSet<ChunkID>,
    },
    DownloadChunk {
        chunk_id: ChunkID,
    },
}

#[derive(Archive, Serialize, Deserialize, Copy, Clone, PartialEq, Hash, Eq)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Hash, Copy, Clone))]
#[repr(transparent)]
pub struct ChunkID {
    id: u128,
}

impl Debug for ChunkID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:#x}", self.id))
    }
}

impl ChunkID {
    pub fn to_bytes(&self) -> [u8; 16] {
        self.id.to_be_bytes()
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

impl std::fmt::Display for ArchivedChunkID {
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

impl From<&ArchivedChunkID> for ChunkID {
    fn from(value: &ArchivedChunkID) -> Self {
        Self { id: value.id }
    }
}

#[derive(Clone, sqlx::FromRow, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq), check_bytes)]
// TODO: change ChunkMetadata hash to bytes, for efficiency
// TODO: encrypt chunk metadata that isn't ID??
pub struct ChunkMetadata {
    pub id: ChunkID,
    pub hash: ChunkHash,
    #[sqlx(rename = "chunk_size")]
    pub size: u32,
    pub indice: u64,
    // TODO: it's best to not send this to the server at all
    pub nonce: EncryptionNonce,
}

impl ChunkMetadata {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;

        println!("ChunkMetadata is {}", bytes.len());
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(2);
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}

#[derive(Archive, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ChunkHash(blake3::Hash);

impl ChunkHash {
    pub fn to_bytes(&self) -> &[u8] {
        self.0.as_bytes()
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

#[derive(Archive, Clone, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
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

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum UploadChunkErr {
    AuthErr,
    InternalServerErr,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum UploadChunkResp {
    Ok,
    Err(UploadChunkErr),
}

impl UploadChunkResp {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(2);
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum DownloadChunkErr {
    AuthErr,
    ChunkNotFound,
    InternalServerErr,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum DownloadChunkResp {
    ChunkData { meta: ChunkMetadata, chunk: Vec<u8> },
    Err(DownloadChunkErr),
}

impl DownloadChunkResp {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(4);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}

pub struct ChunkData {
    pub meta: ChunkMetadata,
    pub chunk: Vec<u8>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum ChunksUploadedErr {
    AuthErr,
    InternalServerErrr,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum ChunksUploadedQueryResp {
    ChunksUploaded { chunks: HashMap<ChunkID, bool> },
    Err(ChunksUploadedErr),
}

impl ChunksUploadedQueryResp {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(4);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}
