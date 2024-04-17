use std::{
    collections::{HashMap, HashSet},
    env,
};

use anyhow::Result;
use async_trait::async_trait;
use bfsp::{ChunkHash, ChunkID, ChunkMetadata, EncryptedFileMetadata};
use log::debug;
use sqlx::{Execute, PgPool, QueryBuilder, Row};
use thiserror::Error;

#[async_trait]
pub trait ChunkDatabase: Sized {
    type InsertChunkError: std::error::Error;

    async fn new() -> Result<Self>;
    async fn contains_chunk(&self, chunk_id: ChunkID, user_id: i64) -> Result<bool>;
    async fn insert_chunk(
        &self,
        chunk_meta: ChunkMetadata,
        user_id: i64,
    ) -> std::result::Result<(), InsertChunkError>;
    // TODO: add a funtion to get multiple chunsk
    async fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> Result<Option<ChunkMetadata>>;
    async fn delete_chunks(&self, chunk_ids: &HashSet<ChunkID>) -> Result<()>;
    async fn insert_file_meta(
        &self,
        enc_metadata: EncryptedFileMetadata,
        user_id: i64,
    ) -> Result<()>;
    async fn get_file_meta(
        &self,
        meta_id: i64,
        user_id: i64,
    ) -> Result<Option<EncryptedFileMetadata>>;
    async fn list_file_meta(
        &self,
        meta_ids: HashSet<i64>,
        user_id: i64,
    ) -> Result<HashMap<i64, EncryptedFileMetadata>>;
}

pub struct PostgresDB {
    pool: PgPool,
}

#[derive(Debug, Error)]
pub enum InsertChunkError {
    #[error("Chunk already exists")]
    AlreadyExists,
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

#[async_trait]
impl ChunkDatabase for PostgresDB {
    type InsertChunkError = InsertChunkError;

    async fn new() -> Result<Self> {
        let pool = sqlx::PgPool::connect(
            &env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/efs_db".to_string()),
        )
        .await?;

        sqlx::migrate!()
            .run(&pool)
            .await
            .map_err(|err| anyhow::anyhow!("Error running database migrations: {err:?}"))?;

        Ok(PostgresDB { pool })
    }

    async fn contains_chunk(&self, chunk_id: ChunkID, user_id: i64) -> Result<bool> {
        Ok(
            sqlx::query("select id from chunks where id = $1 AND user_id = $2")
                .bind(chunk_id)
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    async fn insert_chunk(
        &self,
        chunk_meta: ChunkMetadata,
        user_id: i64,
    ) -> std::result::Result<(), InsertChunkError> {
        let indice: i64 = chunk_meta.indice.try_into().unwrap();
        let chunk_id: ChunkID = ChunkID::try_from(chunk_meta.id.as_str()).unwrap();
        let chunk_hash: ChunkHash = ChunkHash::from_bytes(chunk_meta.hash.try_into().unwrap());
        let chunk_size: i64 = chunk_meta.size.into();

        if let Err(err) = sqlx::query(
            "insert into chunks (hash, id, chunk_size, indice, nonce, user_id) values ( $1, $2, $3, $4, $5, $6 )",
        )
        .bind(chunk_hash)
        .bind(chunk_id)
        .bind(chunk_size)
        .bind(indice)
        .bind(chunk_meta.nonce)
        .bind(user_id)
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
        user_id: i64,
    ) -> Result<Option<ChunkMetadata>> {
        Ok(sqlx::query(
            "select hash, chunk_size, nonce, indice from chunks where id = $1 and user_id = $2",
        )
        .bind(chunk_id.to_string())
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?
        .map(|chunk_info| {
            let chunk_hash: ChunkHash = chunk_info.get::<String, _>("hash").try_into().unwrap();
            ChunkMetadata {
                id: chunk_id.to_string(),
                hash: chunk_hash.to_bytes().to_vec(),
                size: chunk_info.get::<i64, _>("chunk_size").try_into().unwrap(),
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

    async fn insert_file_meta(
        &self,
        enc_file_meta: EncryptedFileMetadata,
        user_id: i64,
    ) -> Result<()> {
        sqlx::query(
            "insert into file_metadata (encrypted_metadata, user_id, nonce) values ($1, $2, $3)",
        )
        .bind(enc_file_meta.metadata)
        .bind(user_id)
        .bind(enc_file_meta.nonce)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_file_meta(
        &self,
        meta_id: i64,
        user_id: i64,
    ) -> Result<Option<EncryptedFileMetadata>> {
        let row = sqlx::query(
            "select encrypted_metadata, nonce from file_metadata where id = $1 and user_id = $2",
        )
        .bind(meta_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let meta = row.get("encrypted_metadata");
            let nonce: Vec<u8> = row.get("nonce");

            return Ok(Some(EncryptedFileMetadata {
                metadata: meta,
                nonce,
            }));
        } else {
            return Ok(None);
        }
    }

    async fn list_file_meta(
        &self,
        ids: HashSet<i64>,
        user_id: i64,
    ) -> Result<HashMap<i64, EncryptedFileMetadata>> {
        let mut query = QueryBuilder::new(
            "select id, encrypted_metadata, nonce from file_metadata where user_id = $1",
        );
        debug!("id len: {}", ids.len());
        if !ids.is_empty() {
            query.push(" and id in (");
            {
                let mut separated = query.separated(",");
                for id in ids {
                    separated.push(format!("{}", id));
                }
            }
            query.push(")");
        }
        let query = query.build().bind(user_id);
        debug!("Executing query: {}", query.sql());

        let file_meta: HashMap<_, _> = query
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let meta_id: i64 = row.get("id");

                let enc_meta: Vec<u8> = row.get("encrypted_metadata");
                let nonce: Vec<u8> = row.get("nonce");
                (
                    meta_id,
                    EncryptedFileMetadata {
                        nonce,
                        metadata: enc_meta,
                    },
                )
            })
            .collect();

        debug!("Found {} file metadata", file_meta.len());

        Ok(file_meta)
    }
}
