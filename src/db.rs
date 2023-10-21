use std::env;

use anyhow::Result;
use async_trait::async_trait;
use bfsp::{ChunkID, ChunkMetadata};
use sqlx::SqlitePool;

#[async_trait]
pub trait ChunkDatabase: Sized {
    async fn new() -> Result<Self>;
    async fn contains_chunk(&self, chunk_id: ChunkID) -> Result<bool>;
    async fn insert_chunk(&self, chunk_meta: ChunkMetadata) -> Result<()>;
    async fn get_chunk_meta(&self, chunk_id: ChunkID) -> Result<Option<ChunkMetadata>>;
}

pub struct SqliteDB {
    pool: SqlitePool,
}

#[async_trait]
impl ChunkDatabase for SqliteDB {
    async fn new() -> Result<Self> {
        let pool = sqlx::SqlitePool::connect(&env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./data.db".to_string())).await?;

        sqlx::migrate!().run(&pool).await?;

        Ok(SqliteDB { pool })
    }

    async fn contains_chunk(&self, chunk_id: ChunkID) -> Result<bool> {
        Ok(sqlx::query!("select id from chunks where id = ?", chunk_id)
            .fetch_optional(&self.pool)
            .await?
            .is_some())
    }

    async fn insert_chunk(&self, chunk_meta: ChunkMetadata) -> Result<()> {
        let indice: i64 = chunk_meta.indice.try_into().unwrap();

        sqlx::query!(
            "insert into chunks (hash, id, chunk_size, indice, nonce) values ( ?, ?, ?, ?, ? )",
            chunk_meta.hash,
            chunk_meta.id,
            chunk_meta.size,
            indice,
            chunk_meta.nonce
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_chunk_meta(&self, chunk_id: ChunkID) -> Result<Option<ChunkMetadata>> {
        Ok(sqlx::query!(
            "select hash, chunk_size, nonce, indice from chunks where id = ?",
            chunk_id
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|chunk_info| ChunkMetadata {
            id: chunk_id,
            hash: chunk_info.hash.try_into().unwrap(),
            size: chunk_info.chunk_size.try_into().unwrap(),
            indice: chunk_info.indice.try_into().unwrap(),
            nonce: chunk_info.nonce.try_into().unwrap(),
        }))
    }
}
