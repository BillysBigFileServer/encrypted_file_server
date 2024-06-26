use std::{
    collections::{HashMap, HashSet},
    env,
    future::Future,
};

use anyhow::Result;
use bfsp::{ChunkHash, ChunkID, ChunkMetadata, EncryptedFileMetadata};
use sqlx::{PgPool, QueryBuilder, Row};
use thiserror::Error;

pub trait MetaDB: Sized + Send + Sync + std::fmt::Debug {
    type InsertChunkError: std::error::Error;

    fn new() -> impl Future<Output = Result<Self>> + Send;
    fn contains_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> impl Future<Output = Result<bool>> + Send;
    fn insert_chunk_meta(
        &self,
        chunk_meta: ChunkMetadata,
        user_id: i64,
    ) -> impl Future<Output = std::result::Result<(), InsertChunkError>> + Send;
    // TODO: add a funtion to get multiple chunsk
    fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<ChunkMetadata>>> + Send;
    fn delete_chunk_metas(
        &self,
        chunk_ids: &HashSet<ChunkID>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn insert_file_meta(
        &self,
        enc_metadata: EncryptedFileMetadata,
        user_id: i64,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get_file_meta(
        &self,
        meta_id: String,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<EncryptedFileMetadata>>> + Send;
    fn list_file_meta(
        &self,
        meta_ids: HashSet<String>,
        user_id: i64,
    ) -> impl Future<Output = Result<HashMap<String, EncryptedFileMetadata>>> + Send;
    fn list_chunk_meta(
        &self,
        chunk_ids: HashSet<ChunkID>,
        user_id: i64,
    ) -> impl Future<Output = Result<HashMap<ChunkID, ChunkMetadata>>> + Send;
    fn total_usages(
        &self,
        user_id: &[i64],
    ) -> impl Future<Output = Result<HashMap<i64, u64>>> + Send;
    fn list_all_chunk_ids(&self) -> impl Future<Output = Result<HashSet<ChunkID>>> + Send;
    fn delete_file_meta(
        &self,
        meta_id: String,
        user_id: i64,
    ) -> impl Future<Output = Result<()>> + Send;
    fn storage_caps(
        &self,
        user_ids: &[i64],
    ) -> impl Future<Output = Result<HashMap<i64, u64>>> + Send;
    fn set_storage_caps(&self, caps: HashMap<i64, u64>) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug)]
pub struct PostgresMetaDB {
    pool: PgPool,
}

#[derive(Debug, Error)]
pub enum InsertChunkError {
    #[error("Chunk already exists")]
    AlreadyExists,
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

impl MetaDB for PostgresMetaDB {
    type InsertChunkError = InsertChunkError;

    #[tracing::instrument(err)]
    async fn new() -> Result<Self> {
        let pool = sqlx::PgPool::connect(
            &env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/efs_db".to_string()),
        )
        .await?;

        Ok(PostgresMetaDB { pool })
    }

    #[tracing::instrument(err)]
    async fn contains_chunk_meta(&self, chunk_id: ChunkID, user_id: i64) -> Result<bool> {
        Ok(
            sqlx::query("select id from chunks where id = $1 AND user_id = $2")
                .bind(chunk_id)
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    #[tracing::instrument(err)]
    async fn insert_chunk_meta(
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

    #[tracing::instrument(err)]
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

    #[tracing::instrument(err)]
    async fn delete_chunk_metas(&self, chunk_ids: &HashSet<ChunkID>) -> Result<()> {
        if chunk_ids.is_empty() {
            return Ok(());
        }

        let mut query = QueryBuilder::new("delete from chunks where id in (");

        let mut separated = query.separated(",");
        for chunk_id in chunk_ids {
            separated.push(format!("'{}'", chunk_id));
        }

        separated.push_unseparated(")");

        query.build().execute(&self.pool).await?;

        Ok(())
    }

    #[tracing::instrument(err, skip(enc_file_meta))]
    async fn insert_file_meta(
        &self,
        enc_file_meta: EncryptedFileMetadata,
        user_id: i64,
    ) -> Result<()> {
        sqlx::query(
            "insert into file_metadata (id, encrypted_metadata, user_id) values ($1, $2, $3)",
        )
        .bind(enc_file_meta.id)
        .bind(enc_file_meta.metadata)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn get_file_meta(
        &self,
        meta_id: String,
        user_id: i64,
    ) -> Result<Option<EncryptedFileMetadata>> {
        let row = sqlx::query(
            "select encrypted_metadata, id from file_metadata where id = $1 and user_id = $2",
        )
        .bind(meta_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let metadata = row.get("encrypted_metadata");
            let id: String = row.get("id");

            return Ok(Some(EncryptedFileMetadata { metadata, id }));
        } else {
            return Ok(None);
        }
    }

    #[tracing::instrument(err)]
    async fn list_file_meta(
        &self,
        ids: HashSet<String>,
        user_id: i64,
    ) -> Result<HashMap<String, EncryptedFileMetadata>> {
        let mut query = QueryBuilder::new(
            "select id, encrypted_metadata from file_metadata where user_id = $1",
        );
        if !ids.is_empty() {
            query.push(" and id in ('");
            {
                let mut separated = query.separated("',");
                for id in ids {
                    separated.push(id);
                }
            }
            query.push("')");
        }
        println!("{}", query.sql());
        let query = query.build().bind(user_id);

        let file_meta: HashMap<_, _> = query
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let id: String = row.get("id");
                let enc_meta: Vec<u8> = row.get("encrypted_metadata");

                (
                    id.clone(),
                    EncryptedFileMetadata {
                        id,
                        metadata: enc_meta,
                    },
                )
            })
            .collect();

        Ok(file_meta)
    }

    #[tracing::instrument(err)]
    async fn list_chunk_meta(
        &self,
        chunk_ids: HashSet<ChunkID>,
        user_id: i64,
    ) -> Result<HashMap<ChunkID, ChunkMetadata>> {
        let mut query = QueryBuilder::new(
            "select id, hash, chunk_size, nonce, indice from chunks where user_id = $1",
        );
        if !chunk_ids.is_empty() {
            query.push(" and id in (");
            {
                let mut separated = query.separated(",");
                for id in chunk_ids {
                    separated.push(id.to_string());
                }
            }
            query.push(")");
        }
        let query = query.build().bind(user_id);

        let chunk_meta: HashMap<_, _> = query
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let id: String = row.get("id");
                let hash: String = row.get("hash");
                let size: i64 = row.get("chunk_size");
                let nonce: Vec<u8> = row.get("nonce");
                let indice: i64 = row.get("indice");

                let hash = ChunkHash::try_from(hash).unwrap();

                (
                    ChunkID::try_from(id.as_str()).unwrap(),
                    ChunkMetadata {
                        id,
                        hash: hash.to_bytes().to_vec(),
                        size: size.try_into().unwrap(),
                        nonce,
                        indice: indice.try_into().unwrap(),
                    },
                )
            })
            .collect();

        Ok(chunk_meta)
    }

    #[tracing::instrument(err)]
    fn list_all_chunk_ids(&self) -> impl Future<Output = Result<HashSet<ChunkID>>> + Send {
        async move {
            let chunk_meta: HashSet<_> = sqlx::query("select id from chunks")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| {
                    let id: String = row.get("id");
                    ChunkID::try_from(id.as_str()).unwrap()
                })
                .collect();

            Ok(chunk_meta)
        }
    }

    #[tracing::instrument(err)]
    async fn delete_file_meta(&self, meta_id: String, user_id: i64) -> Result<()> {
        sqlx::query("delete from file_metadata where id = $1 and user_id = $2")
            .bind(meta_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn total_usages(&self, user_ids: &[i64]) -> Result<HashMap<i64, u64>> {
        // get the size of all file metadatas
        let mut query = QueryBuilder::new(
            "select sum(length(encrypted_metadata))::bigint as sum, user_id from file_metadata",
        );
        if !user_ids.is_empty() {
            query.push(" where user_id in (");
            {
                let mut separated = query.separated(",");
                for id in user_ids {
                    separated.push(id.to_string());
                }
            }
            query.push(")");
        }
        query.push(" group by user_id");
        let query = query.build();
        let rows = query.fetch_all(&self.pool).await?;
        let mut usages: HashMap<i64, u64> = rows
            .into_iter()
            .map(|row| {
                let sum: i64 = row.get("sum");
                let user_id: i64 = row.get("user_id");

                (user_id.try_into().unwrap(), sum.try_into().unwrap())
            })
            .collect();

        let mut query =
            QueryBuilder::new("select sum(chunk_size)::bigint as sum, user_id from chunks");

        if !user_ids.is_empty() {
            query.push(" where user_id in (");
            {
                let mut separated = query.separated(",");
                for id in user_ids {
                    separated.push(id.to_string());
                }
            }
            query.push(")");
        }
        query.push(" group by user_id");
        let query = query.build();
        let rows = query.fetch_all(&self.pool).await?;

        rows.into_iter().for_each(|row| {
            let sum: i64 = row.get("sum");
            let user_id: i64 = row.get("user_id");

            if let Some(usage) = usages.get_mut(&user_id) {
                let sum: u64 = sum.try_into().unwrap();
                *usage += sum;
            } else {
                usages.insert(user_id.try_into().unwrap(), sum.try_into().unwrap());
            }
        });

        Ok(usages)
    }

    #[tracing::instrument(err)]
    async fn storage_caps(&self, user_ids: &[i64]) -> Result<HashMap<i64, u64>> {
        let mut query = QueryBuilder::new("select max_bytes, user_id from storage_caps");

        if !user_ids.is_empty() {
            query.push(" where user_id in (");
            {
                let mut separated = query.separated(",");
                for id in user_ids {
                    separated.push(id.to_string());
                }
            }
            query.push(")");
        }
        query.push(" group by user_id");
        let query = query.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut caps: HashMap<i64, u64> = rows
            .into_iter()
            .map(|row| {
                let storage_cap: i64 = row.get("max_bytes");
                let user_id: i64 = row.get("user_id");

                (user_id.try_into().unwrap(), storage_cap.try_into().unwrap())
            })
            .collect();

        // 5 GiB
        const DEFAULT_CAP: u64 = 5 * 1024 * 1024 * 1024;

        user_ids.iter().for_each(|id| {
            if !caps.contains_key(id) {
                caps.insert(*id, DEFAULT_CAP);
            }
        });

        Ok(caps)
    }

    #[tracing::instrument(err)]
    async fn set_storage_caps(&self, caps: HashMap<i64, u64>) -> Result<()> {
        let mut query = QueryBuilder::new("insert into storage_caps (user_id, max_bytes) values ");

        let mut separated = query.separated(",");
        for (user_id, cap) in caps {
            println!("{} {}", user_id, cap);
            separated.push(format!("({}, {})", user_id, cap));
        }

        query.push(" on conflict (user_id) do update set max_bytes = excluded.max_bytes");

        let query = query.build();

        query.execute(&self.pool).await?;

        Ok(())
    }
}
