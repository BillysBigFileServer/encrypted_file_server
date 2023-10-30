pub use crate::crypto::*;

use anyhow::{anyhow, Error, Result};
use blake3::Hasher;
use sqlx::{sqlite::SqliteRow, Row, Sqlite};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io::SeekFrom,
    str::FromStr,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use uuid::Uuid;

//TODO: maybe include all the data being sent in the enum itself?
pub enum Action {
    UploadChunk = 0,
    QueryChunksUploaded = 1,
    DownloadChunk = 2,
}

impl TryFrom<u16> for Action {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Action::UploadChunk),
            1 => Ok(Action::QueryChunksUploaded),
            2 => Ok(Action::DownloadChunk),
            _ => Err(anyhow!("Invalid action")),
        }
    }
}

impl From<Action> for u16 {
    fn from(value: Action) -> Self {
        value as u16
    }
}

use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Copy, Clone, PartialEq, Hash, Eq)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Hash, Copy, Clone))]
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

#[derive(Clone, Debug, PartialEq)]
pub struct FileHeader {
    pub hash: FileHash,
    pub chunk_size: u32,
    pub chunks: HashMap<ChunkID, ChunkMetadata>,
    pub chunk_indices: HashMap<ChunkID, u64>,
}

impl FileHeader {
    pub fn total_file_size(&self) -> u64 {
        self.chunks.values().map(|chunk| chunk.size as u64).sum()
    }
}

impl FileHeader {
    /// Random file ID
    pub async fn from_file(file: &mut File) -> Result<Self> {
        file.rewind().await?;

        let metadata = file.metadata().await?;
        let size = metadata.len();
        let chunk_size = match size {
            // &KiB for anything less than 256 KiB
            0..=262_144 => 8192,
            // 64KiB for anything up to 8MiB
            262_145..=8388608 => 65536,
            // 8MiB for anything higher
            _ => 8388608,
        };

        let mut total_file_hasher = Hasher::new();
        // Use parallel hashing for data larger than 256KiB
        let use_parallel_chunk = use_parallel_hasher(chunk_size);

        let mut chunk_buf = vec![0; chunk_size];
        let mut chunk_buf_index = 0;

        let mut chunks = HashMap::new();
        let mut chunk_indices = HashMap::new();
        let mut chunk_index = 0;

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

            let mut chunk_hasher = Hasher::new();
            // Next, take the hash of the chunk that was read
            match use_parallel_chunk {
                true => {
                    total_file_hasher.update_rayon(chunk_buf);
                    chunk_hasher.update_rayon(chunk_buf);
                }
                false => {
                    total_file_hasher.update(chunk_buf);
                    chunk_hasher.update(chunk_buf);
                }
            };

            let chunk_hash: ChunkHash = chunk_hasher.finalize().into();
            let chunk_id = ChunkID::new(&chunk_hash);
            let nonce = EncryptionNonce::new(&chunk_hash);

            // Finally, insert some chunk metadata
            chunks.insert(
                chunk_id,
                ChunkMetadata {
                    id: chunk_id,
                    indice: chunk_index,
                    hash: chunk_hash,
                    size: chunk_buf.len() as u32,
                    nonce,
                },
            );
            chunk_indices.insert(chunk_id, chunk_index);

            chunk_index += 1;

            if eof {
                break;
            }
            chunk_buf_index = 0;
        }

        file.rewind().await?;

        Ok(Self {
            hash: total_file_hasher.finalize().into(),
            chunk_size: chunk_size as u32,
            chunks,
            chunk_indices,
        })
    }
}

//TODO: can this be a slice?
pub async fn compressed_encrypted_chunk_from_file(
    file_header: &FileHeader,
    file: &mut File,
    chunk_id: ChunkID,
    key: &EncryptionKey,
) -> Result<Vec<u8>> {
    let chunk_meta = file_header
        .chunks
        .get(&chunk_id)
        .ok_or_else(|| anyhow!("Chunk not found"))?;

    let chunk_indice = *file_header
        .chunk_indices
        .get(&chunk_id)
        .ok_or_else(|| anyhow!("Chunk not found"))?;

    let byte_index = chunk_indice as u64 * file_header.chunk_size as u64;

    file.seek(SeekFrom::Start(byte_index)).await?;

    let mut buf = Vec::with_capacity(chunk_meta.size as usize);
    file.take(chunk_meta.size as u64)
        .read_to_end(&mut buf)
        .await?;

    println!("Size before compression: {}KB", buf.len());

    let mut buf = zstd::bulk::compress(&buf, 15)?;
    println!("Size after compression: {}KB", buf.len());

    file.rewind().await?;

    key.encrypt_chunk_in_place(&mut buf, &chunk_meta)?;

    Ok(buf)
}

pub const fn use_parallel_hasher(size: usize) -> bool {
    size > 262_144
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct ChunksUploadedQuery {
    pub chunks: HashSet<ChunkID>,
}

impl ChunksUploadedQuery {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(2 + bytes.len());
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            println!("ChunksUploadedQuery is {} bytes", bytes.len());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct ChunksUploaded {
    pub chunks: HashMap<ChunkID, bool>,
}

impl ChunksUploaded {
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

    pub fn try_from_bytes(bytes: &[u8]) -> Result<&ArchivedChunksUploaded> {
        rkyv::check_archived_root::<Self>(bytes)
            .map_err(|_| anyhow!("Error deserializing PartsUploaded"))
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct DownloadChunkReq {
    pub chunk_id: ChunkID,
}

impl DownloadChunkReq {
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
pub struct DownloadReq {
    pub file_id: u128,
    pub chunks: Vec<ChunkID>,
}

impl DownloadReq {
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

    pub fn try_from_bytes(bytes: &[u8]) -> Result<&ArchivedDownloadReq> {
        rkyv::check_archived_root::<Self>(bytes)
            .map_err(|_| anyhow!("Error deserializing PartsUploaded"))
    }
}
