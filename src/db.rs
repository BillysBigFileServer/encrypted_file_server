use std::env;

use anyhow::Result;
use async_trait::async_trait;
use bfsp::{ChunkID, ChunkMetadata};
use sqlx::{Row, SqlitePool};

#[async_trait]
pub trait ChunkDatabase: Sized {
    async fn new() -> Result<Self>;
    async fn contains_chunk(&self, chunk_id: ChunkID, username: &str) -> Result<bool>;
    async fn insert_chunk(&self, chunk_meta: ChunkMetadata, username: &str) -> Result<()>;
    async fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        username: &str,
    ) -> Result<Option<ChunkMetadata>>;
}

pub struct SqliteDB {
    pool: SqlitePool,
}

#[async_trait]
impl ChunkDatabase for SqliteDB {
    async fn new() -> Result<Self> {
        let pool = sqlx::SqlitePool::connect(
            &env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./data.db".to_string()),
        )
        .await?;

        sqlx::migrate!().run(&pool).await?;

        Ok(SqliteDB { pool })
    }

    async fn contains_chunk(&self, chunk_id: ChunkID, username: &str) -> Result<bool> {
        Ok(
            sqlx::query("select id from chunks where id = ? AND username = ?")
                .bind(chunk_id)
                .bind(username)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    async fn insert_chunk(&self, chunk_meta: ChunkMetadata, username: &str) -> Result<()> {
        let indice: i64 = chunk_meta.indice.try_into().unwrap();

        sqlx::query(
            "insert into chunks (hash, id, chunk_size, indice, nonce, username) values ( ?, ?, ?, ?, ?, ? )",
        )
        .bind(chunk_meta.hash)
        .bind(chunk_meta.id)
        .bind(chunk_meta.size)
        .bind(indice)
        .bind(chunk_meta.nonce)
        .bind(username)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        username: &str,
    ) -> Result<Option<ChunkMetadata>> {
        Ok(sqlx::query(
            "select hash, chunk_size, nonce, indice from chunks where id = ? and username = ?",
        )
        .bind(chunk_id)
        .bind(username)
        .fetch_optional(&self.pool)
        .await?
        .map(|chunk_info| ChunkMetadata {
            id: chunk_id,
            hash: chunk_info.get::<String, _>("hash").try_into().unwrap(),
            size: chunk_info.get::<u32, _>("chunk_size"),
            indice: chunk_info.get::<i64, _>("indice").try_into().unwrap(),
            nonce: chunk_info.get::<Vec<u8>, _>("nonce").try_into().unwrap(),
        }))
    }
}
