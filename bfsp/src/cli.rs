use anyhow::{anyhow, Result};
use blake3::Hasher;
use std::{collections::HashMap, io::SeekFrom};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{ChunkHash, ChunkID, ChunkMetadata, EncryptionKey, EncryptionNonce, FileHash};

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
            // 8KiB for anything less than 256 KiB
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
