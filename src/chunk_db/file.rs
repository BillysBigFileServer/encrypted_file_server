use std::{collections::HashSet, sync::Arc};

use bfsp::ChunkID;
use tokio::fs;

use crate::meta_db::MetaDB;

use super::ChunkDB;

pub struct FSChunkDB;

impl ChunkDB for FSChunkDB {
    fn new() -> anyhow::Result<Self> {
        Ok(Self)
    }

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

    async fn put_chunk(&self, chunk_id: &ChunkID, user_id: i64, data: &[u8]) -> anyhow::Result<()> {
        let path = Self::get_path(chunk_id, user_id).await;
        fs::write(path, data).await?;
        Ok(())
    }

    async fn delete_chunk(&self, chunk_id: &ChunkID, user_id: i64) -> anyhow::Result<()> {
        let path = Self::get_path(chunk_id, user_id).await;
        fs::remove_file(path).await?;

        Ok(())
    }

    async fn get_path(chunk_id: &ChunkID, user_id: i64) -> String {
        let mut path = format!("./chunks/{user_id}/");
        fs::create_dir_all(&path).await.unwrap();

        path.push_str(&chunk_id.to_string());
        path
    }

    async fn garbage_collect(&self, meta_db: Arc<impl MetaDB>) -> anyhow::Result<()> {
        let chunk_ids = meta_db.list_all_chunk_ids().await?;
        let chunk_ids = chunk_ids
            .iter()
            .map(|c| c.id.clone())
            .collect::<HashSet<_>>();

        let mut files = fs::read_dir("./chunks").await?;
        while let Some(file) = files.next_entry().await? {
            let file_name = file.file_name().into_string().unwrap();
            if !chunk_ids.contains(&ChunkID::try_from(file_name.as_str()).unwrap().id) {
                fs::remove_file(file.path()).await?;
            }
        }

        Ok(())
    }
}
