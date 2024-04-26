//pub mod file;
pub mod s3;

use bfsp::ChunkID;
use std::{collections::HashMap, future::Future};

pub trait ChunkDB: Sized + Send + Sync {
    fn new() -> anyhow::Result<Self>;
    fn get_chunk(
        &self,
        chunk_id: &ChunkID,
        user_id: i64,
    ) -> impl Future<Output = anyhow::Result<Option<Vec<u8>>>> + Send;
    fn put_chunk(
        &self,
        chunk_id: &ChunkID,
        user_id: i64,
        data: &[u8],
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn delete_chunk(
        &self,
        chunk_id: &ChunkID,
        user_id: i64,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_path(chunk_id: &ChunkID, user_id: i64) -> impl Future<Output = String> + Send;
}
