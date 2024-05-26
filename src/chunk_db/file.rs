use std::{collections::HashSet, sync::Arc};

use bfsp::ChunkID;
use tokio::fs;

use crate::meta_db::MetaDB;

use super::ChunkDB;

#[derive(Debug)]
pub struct FSChunkDB;

impl ChunkDB for FSChunkDB {
    #[tracing::instrument]
    fn new() -> anyhow::Result<Self> {
        std::fs::create_dir_all("./chunks").unwrap();
        Ok(Self)
    }

    #[tracing::instrument(err)]
    async fn get_chunk(&self, chunk_id: &ChunkID, user_id: i64) -> anyhow::Result<Option<Vec<u8>>> {
        let path = Self::get_path(chunk_id, user_id).await;
        match fs::read(path).await {
            Ok(data) => Ok(Some(data)),
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(err.into()),
            },
        }
    }

    #[tracing::instrument(err)]
    async fn put_chunk(&self, chunk_id: &ChunkID, user_id: i64, data: &[u8]) -> anyhow::Result<()> {
        let path = Self::get_path(chunk_id, user_id).await;
        fs::write(path, data).await?;
        Ok(())
    }

    #[tracing::instrument(err)]
    async fn delete_chunk(&self, chunk_id: &ChunkID, user_id: i64) -> anyhow::Result<()> {
        let path = Self::get_path(chunk_id, user_id).await;
        fs::remove_file(path).await?;

        Ok(())
    }

    #[tracing::instrument]
    async fn get_path(chunk_id: &ChunkID, user_id: i64) -> String {
        let mut path = format!("./chunks/{user_id}/");
        fs::create_dir_all(&path).await.unwrap();

        path.push_str(&chunk_id.to_string());
        path
    }

    #[tracing::instrument(err)]
    async fn garbage_collect(&self, meta_db: Arc<impl MetaDB>) -> anyhow::Result<()> {
        Ok(())
    }
}
