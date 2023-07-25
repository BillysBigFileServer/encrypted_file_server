/// Billy's file sync protocol

#[cfg(test)]
mod test;

use anyhow::{anyhow, Result};
use blake3::Hasher;
use std::{collections::HashMap, io::SeekFrom};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

pub enum Action {
    Upload,
}

impl TryFrom<u16> for Action {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Action::Upload),
            _ => Err(anyhow!("Invalid action")),
        }
    }
}

impl From<Action> for u16 {
    fn from(value: Action) -> Self {
        match value {
            Action::Upload => 0,
        }
    }
}

use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

pub type ChunkID = u64;

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ChunkMetadata {
    pub id: ChunkID,
    pub hash: String,
    pub size: u32,
}

impl ChunkMetadata {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        debug_assert!(*rkyv::check_archived_root::<ChunkMetadata>(&bytes).unwrap() == self.clone());

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

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct FileHeader {
    pub hash: String,
    pub chunk_size: u32,
    pub chunks: HashMap<ChunkID, ChunkMetadata>,
}

impl ArchivedFileHeader {
    pub fn total_file_size(&self) -> u64 {
        self.chunks.values().map(|chunk| chunk.size as u64).sum()
    }
}

impl FileHeader {
    pub async fn from_file(file: &mut File) -> Result<Self> {
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
        let mut chunk_id = 0;

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

            // Finally, insert some chunk metadata
            chunks.insert(
                chunk_id,
                ChunkMetadata {
                    id: chunk_id,
                    hash: chunk_hasher.finalize().to_string(),
                    size: chunk_buf_index as u32,
                },
            );
            chunk_id += 1;

            if eof {
                break;
            }
            chunk_buf_index = 0;
        }

        Ok(Self {
            hash: total_file_hasher.finalize().to_string(),
            chunk_size: chunk_size as u32,
            chunks,
        })
    }

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

pub async fn chunk_from_file(
    file_header: &FileHeader,
    file: &mut File,
    chunk_id: ChunkID,
) -> Result<Vec<u8>> {
    let chunk_meta = file_header
        .chunks
        .get(&chunk_id)
        .ok_or_else(|| anyhow!("Chunk not found"))?;
    let byte_index = chunk_id * file_header.chunk_size as u64;

    file.seek(SeekFrom::Start(byte_index)).await?;

    let mut buf = Vec::with_capacity(chunk_meta.size as usize);
    file.take(chunk_meta.size as u64)
        .read_to_end(&mut buf)
        .await?;

    Ok(buf)
}

pub fn use_parallel_hasher(size: usize) -> bool {
    size > 262_144
}
