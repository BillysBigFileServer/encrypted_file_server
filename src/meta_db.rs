use std::{
    collections::{HashMap, HashSet},
    env,
    future::Future,
};

use anyhow::Result;
use bfsp::{
    internal::{ActionInfo, Suspension},
    prost_types, ChunkHash, ChunkID, ChunkMetadata, EncryptedChunkMetadata, EncryptedFileMetadata,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    types::{time::OffsetDateTime, Json},
    Executor, PgPool, QueryBuilder, Row,
};
use thiserror::Error;

// 1 GiB
const DEFAULT_CAP: i64 = 1 * 1024 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
struct SuspensionInfoJSON {
    read_suspended: Option<bool>,
    write_suspended: Option<bool>,
    delete_suspended: Option<bool>,
    query_suspended: Option<bool>,
}

impl From<SuspensionInfoJSON> for Suspension {
    fn from(s: SuspensionInfoJSON) -> Self {
        Self {
            read_suspended: s.read_suspended.unwrap_or_default(),
            query_suspended: s.query_suspended.unwrap_or_default(),
            write_suspended: s.write_suspended.unwrap_or_default(),
            delete_suspended: s.delete_suspended.unwrap_or_default(),
        }
    }
}

impl From<Suspension> for SuspensionInfoJSON {
    fn from(s: Suspension) -> Self {
        Self {
            read_suspended: Some(s.read_suspended),
            query_suspended: Some(s.query_suspended),
            write_suspended: Some(s.write_suspended),
            delete_suspended: Some(s.delete_suspended),
        }
    }
}

pub trait MetaDB: Sized + Send + Sync + std::fmt::Debug {
    type InsertChunkError: std::error::Error;

    fn new() -> impl Future<Output = Result<Self>> + Send;
    fn contains_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> impl Future<Output = Result<bool>> + Send;
    fn insert_enc_chunk_meta(
        &self,
        enc_chunk_meta: EncryptedChunkMetadata,
        enc_chunk_size: i64,
        user_id: i64,
    ) -> impl Future<Output = std::result::Result<(), InsertChunkError>> + Send;
    // TODO: add a funtion to get multiple chunsk
    fn get_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<ChunkMetadata>>> + Send;
    fn get_enc_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<EncryptedChunkMetadata>>> + Send;
    fn delete_chunk_metas(
        &self,
        chunk_ids: &HashSet<ChunkID>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn insert_file_meta(
        &self,
        enc_metadata: EncryptedFileMetadata,
        user_id: i64,
    ) -> impl Future<Output = Result<()>> + Send;
    fn update_file_meta(
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
    fn list_chunk_ids(&self, user_id: i64)
        -> impl Future<Output = Result<HashSet<ChunkID>>> + Send;
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
    fn suspensions(
        &self,
        user_ids: &[i64],
    ) -> impl Future<Output = Result<HashMap<i64, Suspension>>> + Send;
    fn set_suspensions(
        &self,
        user_suspensions: HashMap<i64, Suspension>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn delete_all_meta(&self, user_id: i64) -> impl Future<Output = Result<()>> + Send;
    fn list_actions(
        &self,
        status: Option<String>,
        begin_executing: bool,
    ) -> impl Future<Output = Result<Vec<ActionInfo>>>;
    fn executed_action(&self, action_id: i32) -> impl Future<Output = Result<()>> + Send;
    fn queue_action(&self, action: ActionInfo) -> impl Future<Output = Result<ActionInfo>> + Send;
    fn delete_action(&self, action_id: i32) -> impl Future<Output = Result<()>> + Send;
    fn get_actions_for_users(
        &self,
        user_ids: HashSet<i64>,
    ) -> impl Future<Output = Result<HashMap<i64, Vec<ActionInfo>>>> + Send;
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

    #[tracing::instrument(err, skip(chunk_meta))]
    async fn insert_enc_chunk_meta(
        &self,
        chunk_meta: EncryptedChunkMetadata,
        enc_chunk_size: i64,
        user_id: i64,
    ) -> std::result::Result<(), InsertChunkError> {
        let chunk_id: ChunkID = ChunkID::try_from(chunk_meta.id.as_str()).unwrap();

        if let Err(err) = sqlx::query(
            "insert into chunks (id, user_id, enc_chunk_size, encrypted_metadata) values ( $1, $2, $3, $4 )",
        )
        .bind(chunk_id)
        .bind(user_id)
        .bind(enc_chunk_size)
        .bind(chunk_meta.enc_metadata)
        .execute(&self.pool)
        .await
        {
            if let sqlx::Error::Database(db_err) = &err {
                if db_err.is_unique_violation() {
                    return Err(InsertChunkError::AlreadyExists);
                } else {
                    return Err(err.into());
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
            "select chunk_size, nonce, hash, indice from chunks where id = $1 and user_id = $2",
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
    async fn get_enc_chunk_meta(
        &self,
        chunk_id: ChunkID,
        user_id: i64,
    ) -> Result<Option<EncryptedChunkMetadata>> {
        Ok(
            sqlx::query("select encrypted_metadata from chunks where id = $1 and user_id = $2")
                .bind(chunk_id.to_string())
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?
                .map(
                    |chunk_info| match chunk_info.try_get::<Vec<u8>, _>("encrypted_metadata") {
                        Ok(enc_metadata) => Some(EncryptedChunkMetadata {
                            id: chunk_id.to_string(),
                            enc_metadata,
                        }),
                        Err(_) => None,
                    },
                )
                .flatten(),
        )
    }

    #[tracing::instrument(err)]
    async fn delete_chunk_metas(&self, chunk_ids: &HashSet<ChunkID>) -> Result<()> {
        if chunk_ids.is_empty() {
            return Ok(());
        }

        let mut query = QueryBuilder::new("delete from chunks where id in (");

        let mut separated = query.separated(",");
        for chunk_id in chunk_ids {
            separated.push_bind(chunk_id.to_string());
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

    #[tracing::instrument(err, skip(enc_file_meta))]
    async fn update_file_meta(
        &self,
        enc_file_meta: EncryptedFileMetadata,
        user_id: i64,
    ) -> Result<()> {
        sqlx::query(
            "update file_metadata set id = $1, encrypted_metadata = $2, user_id = $3 where id = $1",
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
            query.push(" and id in (");
            {
                let mut separated = query.separated(",");
                for id in ids {
                    separated.push_bind(id);
                }
            }
            query.push("')");
        }
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
                    separated.push_bind(id);
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
        // we also get the file_metadata and chunk_metadata as part of the total size, since users can upload whatever there
        let mut query = QueryBuilder::new(
            "
SELECT
  user_id,
  SUM(total_usage)::bigint AS total_usage
FROM
  (
    SELECT
      user_id,
      SUM(LENGTH(encrypted_metadata))::BIGINT AS total_usage
    FROM
      file_metadata
    GROUP BY
      user_id
    UNION ALL
    SELECT
      user_id,
      SUM(
        enc_chunk_size + COALESCE(LENGTH(encrypted_metadata), 0)
      )::BIGINT AS total_usage
    FROM
      chunks
    GROUP BY
      user_id
  ) AS combined_usage",
        );
        if !user_ids.is_empty() {
            query.push(" where user_id in (");
            {
                let mut separated = query.separated(",");
                for id in user_ids {
                    separated.push_bind(id);
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
                let sum: i64 = row.get("total_usage");
                let user_id: i64 = row.get("user_id");

                (user_id.try_into().unwrap(), sum.try_into().unwrap())
            })
            .collect();

        user_ids.iter().for_each(|id| {
            if !usages.contains_key(id) {
                usages.insert(*id, 0);
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
                    separated.push_bind(id);
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

        user_ids.iter().for_each(|id| {
            if !caps.contains_key(id) {
                caps.insert(*id, DEFAULT_CAP.try_into().unwrap());
            }
        });

        Ok(caps)
    }

    #[tracing::instrument(err)]
    async fn set_storage_caps(&self, caps: HashMap<i64, u64>) -> Result<()> {
        let mut query = QueryBuilder::new("insert into storage_caps (user_id, max_bytes) values ");

        let mut separated = query.separated(",");
        for (user_id, cap) in caps {
            separated.push(format!("({}, {})", user_id, cap));
        }

        query.push(" on conflict (user_id) do update set max_bytes = excluded.max_bytes");

        let query = query.build();

        query.execute(&self.pool).await?;

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn suspensions(&self, user_ids: &[i64]) -> Result<HashMap<i64, Suspension>> {
        let mut query = QueryBuilder::new("select user_id, suspension_info from storage_caps");

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

        let mut suspensions: HashMap<i64, Suspension> = rows
            .into_iter()
            .map(|row| {
                let user_id: i64 = row.get("user_id");
                let suspension_info_json: Option<Json<SuspensionInfoJSON>> =
                    row.get("suspension_info");
                let suspension_info: Suspension = suspension_info_json
                    .map(|info| info.0.into())
                    .unwrap_or_default();

                (user_id.try_into().unwrap(), suspension_info)
            })
            .collect();

        user_ids.iter().for_each(|id| {
            if !suspensions.contains_key(id) {
                suspensions.insert(*id, Suspension::default());
            }
        });

        Ok(suspensions)
    }

    #[tracing::instrument(err)]
    async fn set_suspensions(&self, suspensions: HashMap<i64, Suspension>) -> Result<()> {
        let mut query = QueryBuilder::new(
            "insert into storage_caps (user_id, suspension_info, max_bytes) values (",
        );

        let mut separated = query.separated(",");
        for (user_id, suspension) in suspensions {
            let suspension_info_json: SuspensionInfoJSON = suspension.into();
            let suspension_info_json = serde_json::to_string(&suspension_info_json).unwrap();

            separated.push_bind_unseparated(user_id);
            separated.push_unseparated(",");
            separated.push_bind_unseparated(suspension_info_json);
            separated.push_unseparated("::jsonb,");
            separated.push_bind(DEFAULT_CAP);
        }

        query.push(
            ") on conflict (user_id) do update set suspension_info = excluded.suspension_info",
        );

        let query = query.build();

        query.execute(&self.pool).await?;

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn delete_all_meta(&self, user_id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("delete from chunks where user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("delete from file_metadata where user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        // TODO(billy): should i remove suspensions too?

        Ok(())
    }
    #[tracing::instrument(err)]
    async fn list_actions(
        &self,
        status: Option<String>,
        begin_executing: bool,
    ) -> Result<Vec<ActionInfo>> {
        let mut query = QueryBuilder::new("");
        if !begin_executing {
            query.push("select id, action, user_id, execute_at, status from queued_actions");

            if let Some(status) = &status {
                query.push(" where status = ");
                query.push_bind(status);
            }
            query.push(";");
        } else {
            query.push("update queued_actions set status = 'executing'");
            if let Some(status) = &status {
                query.push(" where status = ");
                query.push_bind(status);
                query.push(" and NOW() > execute_at ");
            }
            query.push(" returning id, action, user_id, execute_at, status;");
        }

        let query = query.build();
        Ok(query
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let id = row.get("id");
                let action = row.get("action");
                let user_id = row.get("user_id");
                let execute_at: OffsetDateTime = row.get("execute_at");
                let status = row.get("status");

                ActionInfo {
                    id,
                    action,
                    execute_at: Some(
                        prost_types::Timestamp::date_time_nanos(
                            execute_at.year().into(),
                            execute_at.month().into(),
                            execute_at.day(),
                            execute_at.hour(),
                            execute_at.minute(),
                            execute_at.second(),
                            execute_at.nanosecond(),
                        )
                        .unwrap(),
                    ),
                    status,
                    user_id,
                }
            })
            .collect())
    }

    #[tracing::instrument(err)]
    async fn executed_action(&self, action_id: i32) -> Result<()> {
        let mut query =
            QueryBuilder::new("update queued_actions set status = 'executed' where id = ");
        query.push_bind(action_id);

        let query = query.build();
        self.pool.execute(query).await?;

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn list_chunk_ids(&self, user_id: i64) -> Result<HashSet<ChunkID>> {
        Ok(sqlx::query("select id from chunks where user_id = $1")
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let id: String = row.get("id");
                ChunkID::try_from(id.as_str()).unwrap()
            })
            .collect())
    }

    #[tracing::instrument(err)]
    async fn queue_action(&self, mut action: ActionInfo) -> Result<ActionInfo> {
        let mut query =
            QueryBuilder::new("insert into queued_actions (action, user_id, execute_at, status");

        if action.id.is_some() {
            query.push(", id");
        }
        query.push(") values (");

        let unix_time_nanos = (action.execute_at.unwrap().seconds as i128) * 10_i128.pow(9)
            + action.execute_at.unwrap().nanos as i128;
        let timestamp: OffsetDateTime =
            OffsetDateTime::from_unix_timestamp_nanos(unix_time_nanos).unwrap();

        let mut separated = query.separated(",");

        separated.push_bind(action.action.clone());
        separated.push_bind(action.user_id);
        separated.push_bind(timestamp);
        separated.push_bind(action.status.clone());

        if let Some(id) = action.id {
            separated.push_bind(id);
        }

        query.push(") returning id");

        let row = query.build().fetch_one(&self.pool).await?;
        let id: i32 = row.get("id");

        action.id = Some(id);
        Ok(action)
    }

    #[tracing::instrument(err)]
    async fn delete_action(&self, action_id: i32) -> Result<()> {
        sqlx::query("update queued_actions set status = 'deleted' where id = $1")
            .bind(action_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[tracing::instrument(err)]
    async fn get_actions_for_users(
        &self,
        user_ids: HashSet<i64>,
    ) -> Result<HashMap<i64, Vec<ActionInfo>>> {
        let mut query = QueryBuilder::new(
            "select id, action, user_id, execute_at, status from queued_actions where user_id in (",
        );
        let mut separated = query.separated(",");
        for id in user_ids.iter() {
            separated.push_bind(id);
        }

        query.push(");");

        let mut actions: HashMap<i64, Vec<ActionInfo>> = HashMap::new();

        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .for_each(|row| {
                let id: i32 = row.get("id");
                let action: String = row.get("action");
                let user_id: i64 = row.get("user_id");
                let execute_at: OffsetDateTime = row.get("execute_at");
                let status: String = row.get("status");

                let action_info = ActionInfo {
                    id: Some(id),
                    action,
                    execute_at: Some(
                        prost_types::Timestamp::date_time_nanos(
                            execute_at.year().into(),
                            execute_at.month().into(),
                            execute_at.day(),
                            execute_at.hour(),
                            execute_at.minute(),
                            execute_at.second(),
                            execute_at.nanosecond(),
                        )
                        .unwrap(),
                    ),
                    status,
                    user_id,
                };

                let actions = actions.entry(user_id).or_insert_with(|| Vec::new());
                actions.push(action_info);
            });

        for user_id in user_ids.into_iter() {
            if !actions.contains_key(&user_id) {
                actions.insert(user_id, Vec::new());
            }
        }

        Ok(actions)
    }
}
