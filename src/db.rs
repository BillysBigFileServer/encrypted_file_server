use std::{collections::HashSet, env};

use anyhow::Result;
use async_trait::async_trait;
use bfsp::{ChunkHash, ChunkID, ChunkMetadata};
use log::debug;
use sqlx::{QueryBuilder, Row, SqlitePool};
use thiserror::Error;

#[async_trait]
pub trait ChunkDatabase: Sized {
    type InsertChunkError: std::error::Error;

    async fn new() -> Result<Self>;
    async fn contains_chunk(&self, chunk_id: ChunkID, email: &str) -> Result<bool>;
    async fn insert_chunk(
        &self,
        chunk_meta: ChunkMetadata,
        email: &str,
    ) -> std::result::Result<(), InsertChunkError>;
    // TODO: add a funtion to get multiple chunsk
    async fn get_chunk_meta(&self, chunk_id: ChunkID, email: &str)
        -> Result<Option<ChunkMetadata>>;
    async fn delete_chunks(&self, chunk_ids: &HashSet<ChunkID>) -> Result<()>;
    async fn insert_file_meta(&self, enc_metadata: Vec<u8>, email: &str) -> Result<()>;
}

pub struct SqliteDB {
    pool: SqlitePool,
}

#[derive(Debug, Error)]
pub enum InsertChunkError {
    #[error("Chunk already exists")]
    AlreadyExists,
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

#[async_trait]
impl ChunkDatabase for SqliteDB {
    type InsertChunkError = InsertChunkError;

    async fn new() -> Result<Self> {
        let pool = sqlx::SqlitePool::connect(
            &env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./data.db".to_string()),
        )
        .await?;

        sqlx::migrate!().run(&pool).await?;

        Ok(SqliteDB { pool })
    }

    async fn contains_chunk(&self, chunk_id: ChunkID, email: &str) -> Result<bool> {
        Ok(
            sqlx::query("select id from chunks where id = ? AND email = ?")
                .bind(chunk_id)
                .bind(email)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    async fn insert_chunk(
        &self,
        chunk_meta: ChunkMetadata,
        email: &str,
    ) -> std::result::Result<(), InsertChunkError> {
        let indice: i64 = chunk_meta.indice.try_into().unwrap();
        let chunk_id: ChunkID = ChunkID::from_bytes(chunk_meta.id.try_into().unwrap());
        let chunk_hash: ChunkHash = ChunkHash::from_bytes(chunk_meta.hash.try_into().unwrap());

        if let Err(err) = sqlx::query(
            "insert into chunks (hash, id, chunk_size, indice, nonce, email) values ( ?, ?, ?, ?, ?, ? )",
        )
        .bind(chunk_hash)
        .bind(chunk_id)
        .bind(chunk_meta.size)
        .bind(indice)
        .bind(chunk_meta.nonce)
        .bind(email)
        .execute(&self.pool)
        .await {
            if let sqlx::Error::Database(db_err) = &err {
                if db_err.is_unique_violation() {
                    return Err(InsertChunkError::AlreadyExists);
                } else {
                    return Err(err.into())
                }
            }
        };

        Ok(())
    }

    async fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        email: &str,
    ) -> Result<Option<ChunkMetadata>> {
        Ok(sqlx::query(
            "select hash, chunk_size, nonce, indice from chunks where id = ? and email = ?",
        )
        .bind(chunk_id.to_string())
        .bind(email)
        .fetch_optional(&self.pool)
        .await?
        .map(|chunk_info| {
            let chunk_hash: ChunkHash = chunk_info.get::<String, _>("hash").try_into().unwrap();
            ChunkMetadata {
                id: chunk_id.to_bytes().to_vec(),
                hash: chunk_hash.to_bytes().to_vec(),
                size: chunk_info.get::<u32, _>("chunk_size"),
                indice: chunk_info.get::<i64, _>("indice").try_into().unwrap(),
                nonce: chunk_info.get::<Vec<u8>, _>("nonce"),
            }
        }))
    }

    async fn delete_chunks(&self, chunk_ids: &HashSet<ChunkID>) -> Result<()> {
        let mut query = QueryBuilder::new("delete from chunks where id in (");
        {
            let mut separated = query.separated(",");
            for chunk_id in chunk_ids {
                separated.push(format!("'{}'", chunk_id));
            }
        }
        query.push(")");
        debug!("Executing query: {}", query.sql());

        query.build().execute(&self.pool).await?;

        Ok(())
    }

    async fn insert_file_meta(&self, enc_file_meta: Vec<u8>, email: &str) -> Result<()> {
        let meta_id: u32 = rand::random();
        sqlx::query("insert into (meta_id, encrypted_metadata, email) values (?, ?, ?)")
            .bind(meta_id)
            .bind(enc_file_meta)
            .bind(email)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
