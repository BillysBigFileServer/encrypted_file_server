pub mod file;
pub mod s3;

use bfsp::ChunkID;
use std::{future::Future, sync::Arc};

use crate::meta_db::MetaDB;

pub trait ChunkDB: Sized + Send + Sync + std::fmt::Debug {
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
    /// Garbage collect the chunk db, making sure that any differences between meta_db and the storage backend are resolved.
    fn garbage_collect(
        &self,
        meta_db: Arc<impl MetaDB>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
