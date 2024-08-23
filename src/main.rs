mod auth;
mod chunk_db;
mod internal;
mod meta_db;
mod tls;

use anyhow::anyhow;
use auth::{authorize, Right};
use bfsp::base64_decode;
use bfsp::chacha20poly1305::KeyInit;
use bfsp::chacha20poly1305::XChaCha20Poly1305;
use bfsp::list_chunk_metadata_resp::ChunkMetadatas;
use bfsp::list_file_metadata_resp::FileMetadatas;
use bfsp::EncryptedChunkMetadata;
use biscuit_auth::Biscuit;
use biscuit_auth::PublicKey;
use bytes::Bytes;
use chunk_db::ChunkDB;
use internal::handle_internal_connection;
use opentelemetry::trace::noop::NoopTracer;
use opentelemetry::trace::TraceError;
use opentelemetry_otlp::WithExportConfig;
use reqwest::Client;
use std::convert::Infallible;
use std::env;
use std::fmt::Display;
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::{fs, io};
use tracing::{event, Level};
use tracing_opentelemetry::PreSampledTracer;
use tracing_subscriber::filter::filter_fn;
use wtransport::endpoint::IncomingSession;
use wtransport::Endpoint;
use wtransport::Identity;
use wtransport::ServerConfig;

use crate::chunk_db::file::FSChunkDB;
#[cfg(feature = "s3")]
use crate::chunk_db::s3::S3ChunkDB;
use crate::meta_db::{MetaDB, PostgresMetaDB};
use anyhow::Result;
use bfsp::{
    chunks_uploaded_query_resp::{ChunkUploaded, ChunksUploaded},
    download_chunk_resp::ChunkData,
    file_server_message::Message::{
        ChunksUploadedQuery, DeleteChunksQuery, DeleteFileMetadataQuery, DownloadChunkQuery,
        DownloadFileMetadataQuery, GetUsageQuery, ListChunkMetadataQuery, ListFileMetadataQuery,
        UploadChunk, UploadFileMetadata,
    },
    ChunkID, ChunkMetadata, ChunksUploadedQueryResp, DownloadChunkResp, FileServerMessage, Message,
};
use bfsp::{EncryptedFileMetadata, PrependLen};
use tls::get_tls_cert;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::prelude::*;
use warp::Filter;

fn init_tracer() -> Result<opentelemetry_sdk::trace::Tracer, TraceError> {
    let api_key = env::var("HONEYCOMB_API_KEY").unwrap();
    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_headers(HashMap::from([
            ("x-honeycomb-dataset".into(), "big-file-server".into()),
            ("x-honeycomb-team".into(), api_key),
        ]))
        .with_http_client(Client::new())
        .with_endpoint("https://api.honeycomb.io/");

    Ok(opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(opentelemetry_sdk::trace::config().with_resource(
            opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                "service.name",
                "big_file_server",
            )]),
        ))
        .with_exporter(exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap())
}

fn init_subscriber<T: opentelemetry::trace::Tracer + PreSampledTracer + Send + Sync + 'static>(
    tracer: T,
) {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter = filter_fn(|metadata| {
        metadata.target().starts_with("file_server") || metadata.target().starts_with("bfsp")
    });

    let subscriber = tracing_subscriber::Registry::default()
        .with(stdout_log)
        .with(telemetry)
        .with(filter);
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

#[tokio::main]
async fn main() -> Result<()> {
    let key: String = env::var("INTERNAL_KEY")
        .unwrap_or_else(|_| "Kwdl1_CckyprfRki3pKJ6jGXvSzGxp8I1WsWFqJYS3I=".to_string());
    let key: Vec<u8> = base64_decode(&key).unwrap();
    let internal_private_key = XChaCha20Poly1305::new_from_slice(&key).unwrap();

    match env::var("HONEYCOMB_API_KEY").is_ok() {
        true => init_subscriber(init_tracer()?),
        false => init_subscriber(NoopTracer::new()),
    };

    let public_key = env::var("TOKEN_PUBLIC_KEY").unwrap();
    let public_key = PublicKey::from_bytes_hex(&public_key)?;

    let meta_db = Arc::new(
        PostgresMetaDB::new()
            .await
            .map_err(|err| anyhow!("Error initializing database: {err:?}"))
            .unwrap(),
    );

    #[cfg(feature = "prod")]
    let chunk_db = Arc::new(S3ChunkDB::new().unwrap());
    #[cfg(not(feature = "prod"))]
    let chunk_db = Arc::new(FSChunkDB::new().unwrap());

    let chunk_db_clone = Arc::clone(&chunk_db);
    let meta_db_clone = Arc::clone(&meta_db);

    tokio::task::spawn(async move { chunk_db_clone.garbage_collect(meta_db_clone).await });

    let internal_tcp_addr = "[::]:9990".to_socket_addrs().unwrap().next().unwrap();

    let wt_addr = match env::var("FLY_APP_NAME").is_ok() {
        // in order to serve Webtransport (UDP) on Fly, we have to use fly-global-services, which keep in mind is IPV4 ONLY AS OF WRITING
        true => "fly-global-services:9999"
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap(),
        // I <3 ipv6
        false => "[::]:9999".to_socket_addrs().unwrap().next().unwrap(),
    };

    let http_addr = "[::]:9998".to_socket_addrs().unwrap().next().unwrap();

    if cfg!(feature = "prod") {
        let cert_info = get_tls_cert().await?;

        fs::create_dir_all("/etc/letsencrypt/live/big-file-server.fly.dev/").await?;
        fs::write(
            "/etc/letsencrypt/live/big-file-server.fly.dev/chain.pem",
            cert_info.cert_chain_pem,
        )
        .await?;
        fs::write(
            "/etc/letsencrypt/live/big-file-server.fly.dev/privkey.pem",
            cert_info.private_key_pem,
        )
        .await?;
    }

    let (chain_file, key_file) = match cfg!(feature = "prod") {
        true => (
            "/etc/letsencrypt/live/big-file-server.fly.dev/chain.pem",
            "/etc/letsencrypt/live/big-file-server.fly.dev/privkey.pem",
        ),
        false => ("certs/localhost.pem", "certs/localhost-key.pem"),
    };

    let config = ServerConfig::builder()
        .with_bind_address(wt_addr)
        .with_identity(&Identity::load_pemfiles(chain_file, key_file).await.unwrap())
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .allow_migration(true)
        .max_idle_timeout(Some(Duration::from_secs(10)))
        .unwrap()
        .build();

    let server = Endpoint::server(config).unwrap();

    let meta_db_clone = Arc::clone(&meta_db);

    tokio::task::spawn(async move {
        let sock = tokio::net::TcpListener::bind(internal_tcp_addr)
            .await
            .unwrap();

        let meta_db = meta_db_clone.clone();
        loop {
            let internal_private_key = internal_private_key.clone();
            let meta_db = meta_db.clone();

            let (stream, _addr) = sock.accept().await.unwrap();
            tokio::task::spawn(handle_internal_connection(
                stream,
                internal_private_key,
                meta_db,
            ));
        }
    });

    let meta_db_clone = Arc::clone(&meta_db);
    let chunk_db_clone = Arc::clone(&chunk_db);

    tokio::task::spawn(async move {
        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["POST"])
            .allow_headers(vec!["Content-Type", "Content-Length"]);

        let api = warp::post()
            .and(warp::path("api"))
            .and(warp::body::bytes())
            .and(warp::any().map(move || Arc::clone(&meta_db_clone)))
            .and(warp::any().map(move || Arc::clone(&chunk_db_clone)))
            .and(warp::any().map(move || public_key.clone()))
            .and_then(handle_http_connection)
            .with(cors);
        warp::serve(api).run(http_addr).await;
    });

    loop {
        let incoming_session = server.accept().await;
        let meta_db = Arc::clone(&meta_db);
        let chunk_db = Arc::clone(&chunk_db);

        tokio::task::spawn(handle_connection(
            incoming_session,
            public_key,
            meta_db,
            chunk_db,
        ));
    }
}

#[tracing::instrument(err, skip(bytes))]
async fn handle_http_connection<M: MetaDB + 'static, C: ChunkDB + 'static>(
    bytes: Bytes,
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
    public_key: PublicKey,
) -> Result<warp::http::Response<Vec<u8>>, Infallible> {
    let message_bytes = bytes[4..].to_vec();
    let message = FileServerMessage::from_bytes(&message_bytes).unwrap();
    let resp = handle_message(message, public_key, meta_db, chunk_db)
        .await
        .unwrap();
    event!(Level::INFO, response_size = resp.len() as u64, "Responding");

    Ok(warp::http::Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", resp.len().to_string())
        .status(200)
        .body(resp)
        .unwrap())
}

#[tracing::instrument(skip(incoming_session, public_key))]
async fn handle_connection<M: MetaDB + 'static, C: ChunkDB + 'static>(
    incoming_session: IncomingSession,
    public_key: PublicKey,
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
) {
    let session_request = incoming_session.await.unwrap();
    let conn = session_request.accept().await.unwrap();

    loop {
        let bi = conn.accept_bi().await;
        if let Err(err) = bi {
            event!(Level::ERROR, error = ?err, "Error accepting connection");
            return;
        }

        event!(Level::INFO, "Connection established");

        // A single socket can have multiple connections. Multiplexing!
        let (mut write_sock, mut read_sock) = bi.unwrap();
        let meta_db = Arc::clone(&meta_db);
        let chunk_db = Arc::clone(&chunk_db);

        event!(Level::INFO, "Bi-directional connection established");

        tokio::task::spawn(async move {
            loop {
                event!(Level::INFO, "Waiting for message");
                let action_len = match read_sock.read_u32_le().await.map_err(|e| e.kind()) {
                    Ok(len) => len,
                    Err(io::ErrorKind::UnexpectedEof) => {
                        event!(Level::INFO, "Client disconnected");
                        // This is fine, the client disconnected
                        return;
                    }
                    Err(err) => {
                        event!(Level::ERROR, error = ?err, "Error reading message length");
                        return;
                    }
                };

                event!(Level::DEBUG, action_len = action_len, "Got message length");
                // 9 MiB, super arbitrary
                if action_len > 9_437_184 {
                    todo!("Action {action_len} too big :(");
                }

                let command = {
                    let mut action_buf = vec![0; action_len as usize];
                    read_sock.read_exact(&mut action_buf).await.unwrap();
                    FileServerMessage::from_bytes(&action_buf).unwrap()
                };
                event!(Level::INFO, "Deserialized FileServerMessage");
                let resp = handle_message(
                    command,
                    public_key,
                    Arc::clone(&meta_db),
                    Arc::clone(&chunk_db),
                )
                .await
                .unwrap();

                event!(Level::INFO, response_size = resp.len() as u64, "Responding");
                write_sock.write_all(&resp).await.unwrap();
                write_sock.flush().await.unwrap();
                event!(Level::INFO, "Response sent");
            }
        });
    }
}

// TODO: have a type called AuthorizedToken to not fuck up authentication
#[tracing::instrument(err, skip(public_key, command))]
pub async fn handle_message<M: MetaDB + 'static, C: ChunkDB + 'static>(
    command: FileServerMessage,
    public_key: PublicKey,
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
) -> anyhow::Result<Vec<u8>> {
    let authentication = command.auth.unwrap();
    let token = Biscuit::from_base64(&authentication.token, public_key).unwrap();
    event!(Level::INFO, token = ?token, "Deserialized token");

    Ok(match command.message.unwrap() {
        DownloadChunkQuery(query) => {
            match handle_download_chunk(
                meta_db.as_ref(),
                chunk_db.as_ref(),
                &token,
                ChunkID::try_from(query.chunk_id.as_str())?,
            )
            .await
            {
                Ok(Some((enc_meta, meta, data))) => DownloadChunkResp {
                    response: Some(bfsp::download_chunk_resp::Response::ChunkData(ChunkData {
                        chunk_metadata: meta,
                        enc_chunk_metadata: enc_meta,
                        chunk: data,
                    })),
                }
                .encode_to_vec(),
                Ok(None) => DownloadChunkResp {
                    response: Some(bfsp::download_chunk_resp::Response::Err(
                        "ChunkNotFound".to_string(),
                    )),
                }
                .encode_to_vec(),
                Err(err) => todo!("{err}"),
            }
        }
        ChunksUploadedQuery(query) => {
            let chunk_ids = query
                .chunk_ids
                .into_iter()
                .map(|chunk_id| ChunkID::try_from(chunk_id.as_str()).unwrap())
                .collect();
            match query_chunks_uploaded(meta_db.as_ref(), &token, chunk_ids).await {
                Ok(chunks_uploaded) => ChunksUploadedQueryResp {
                    response: Some(bfsp::chunks_uploaded_query_resp::Response::Chunks(
                        ChunksUploaded {
                            chunks: chunks_uploaded
                                .into_iter()
                                .map(|(chunk_id, uploaded)| ChunkUploaded {
                                    chunk_id: chunk_id.to_bytes().to_vec(),
                                    uploaded,
                                })
                                .collect(),
                        },
                    )),
                }
                .encode_to_vec(),
                Err(err) => todo!("Handle error: {err:?}"),
            }
        }
        UploadChunk(msg) => {
            let enc_chunk_metadata = msg.enc_chunk_metadata.unwrap();
            let chunk = msg.chunk;

            match handle_upload_chunk(meta_db, chunk_db, &token, enc_chunk_metadata, chunk).await {
                Ok(_) => bfsp::UploadChunkResp { err: None }.encode_to_vec(),
                Err(err) => todo!("{err}"),
            }
        }
        DeleteChunksQuery(query) => {
            let chunk_ids: HashSet<ChunkID> = query
                .chunk_ids
                .into_iter()
                .map(|chunk_id| ChunkID::try_from(chunk_id.as_str()).unwrap())
                .collect();

            match handle_delete_chunks(meta_db.as_ref(), chunk_db.as_ref(), &token, chunk_ids).await
            {
                Ok(_) => bfsp::DeleteChunksResp { err: None }.encode_to_vec(),
                Err(err) => todo!("{err}"),
            }
        }
        UploadFileMetadata(meta) => {
            let encrypted_file_meta = meta.encrypted_file_metadata.unwrap();
            match handle_upload_file_metadata(meta_db.as_ref(), &token, encrypted_file_meta).await {
                Ok(_) => bfsp::UploadFileMetadataResp { err: None }.encode_to_vec(),
                Err(err) => todo!("{err:?}"),
            }
        }
        DownloadFileMetadataQuery(query) => {
            let meta_id = query.id;
            match handle_download_file_metadata(meta_db.as_ref(), &token, meta_id).await {
                Ok(meta) => bfsp::DownloadFileMetadataResp {
                    response: Some(
                        bfsp::download_file_metadata_resp::Response::EncryptedFileMetadata(meta),
                    ),
                }
                .encode_to_vec(),
                Err(_) => todo!(),
            }
        }
        ListFileMetadataQuery(query) => {
            let meta_ids = query.ids;
            match handle_list_file_metadata(meta_db.as_ref(), &token, meta_ids).await {
                Ok(metas) => bfsp::ListFileMetadataResp {
                    response: Some(bfsp::list_file_metadata_resp::Response::Metadatas(
                        FileMetadatas { metadatas: metas },
                    )),
                }
                .encode_to_vec(),
                Err(_) => todo!(),
            }
        }
        ListChunkMetadataQuery(query) => {
            let meta_ids: HashSet<ChunkID> = query
                .ids
                .into_iter()
                .map(|id| ChunkID::try_from(id.as_str()).unwrap())
                .collect();
            match handle_list_chunk_metadata(meta_db.as_ref(), &token, meta_ids).await {
                Ok(metas) => bfsp::ListChunkMetadataResp {
                    response: Some(bfsp::list_chunk_metadata_resp::Response::Metadatas(
                        ChunkMetadatas {
                            metadatas: metas
                                .into_iter()
                                .map(|(chunk_id, chunk_meta)| (chunk_id.to_string(), chunk_meta))
                                .collect(),
                        },
                    )),
                }
                .encode_to_vec(),
                Err(_) => todo!(),
            }
        }
        DeleteFileMetadataQuery(query) => {
            let meta_id = query.id;
            match handle_delete_file_metadata(meta_db.as_ref(), &token, meta_id).await {
                Ok(_) => bfsp::DeleteFileMetadataResp { err: None }.encode_to_vec(),
                Err(_) => todo!(),
            }
        }
        GetUsageQuery(_query) => match handle_get_usage(meta_db.as_ref(), &token).await {
            Ok(usage) => bfsp::GetUsageResp {
                response: Some(bfsp::get_usage_resp::Response::Usage(
                    bfsp::get_usage_resp::Usage { total_usage: usage },
                )),
            }
            .encode_to_vec(),
            Err(_) => todo!(),
        },
    }
    .prepend_len())
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_download_chunk<M: MetaDB, C: ChunkDB>(
    meta_db: &M,
    chunk_db: &C,
    token: &Biscuit,
    chunk_id: ChunkID,
) -> Result<
    Option<(
        Option<EncryptedChunkMetadata>,
        Option<ChunkMetadata>,
        Vec<u8>,
    )>,
> {
    let user_id = authorize(Right::Read, token, Vec::new(), meta_db)
        .await
        .unwrap();

    if let Some(enc_chunk_meta) = meta_db.get_enc_chunk_meta(chunk_id, user_id).await? {
        let chunk = chunk_db.get_chunk(&chunk_id, user_id).await?.unwrap();
        Ok(Some((Some(enc_chunk_meta), None, chunk)))
    } else if let Some(chunk_meta) = meta_db.get_chunk_meta(chunk_id, user_id).await? {
        let chunk = chunk_db.get_chunk(&chunk_id, user_id).await?.unwrap();
        Ok(Some((None, Some(chunk_meta), chunk)))
    } else {
        return Ok(None);
    }
}

// FIXME: very ddosable by querying many chunks at once
#[tracing::instrument(err, skip(token))]
async fn query_chunks_uploaded<M: MetaDB>(
    meta_db: &M,
    token: &Biscuit,
    chunks: HashSet<ChunkID>,
) -> Result<HashMap<ChunkID, bool>> {
    let user_id = authorize(Right::Query, token, Vec::new(), meta_db)
        .await
        .unwrap();

    let chunks_uploaded: HashMap<ChunkID, bool> =
        futures::future::join_all(chunks.into_iter().map(|chunk_id| async move {
            let contains_chunk: bool = meta_db
                .contains_chunk_meta(chunk_id, user_id)
                .await
                .unwrap();
            (chunk_id, contains_chunk)
        }))
        .await
        .into_iter()
        .collect();

    Ok(chunks_uploaded)
}

// TODO: Maybe store upload_chunk messages in files and mmap them?
#[tracing::instrument(err, skip(chunk, token))]
async fn handle_upload_chunk<M: MetaDB + 'static, C: ChunkDB + 'static>(
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
    token: &Biscuit,
    enc_chunk_metadata: EncryptedChunkMetadata,
    chunk: Vec<u8>,
) -> Result<()> {
    let user_id = authorize(Right::Write, token, Vec::new(), meta_db.as_ref())
        .await
        .unwrap();

    // 8MiB(?)
    if chunk.len() > 1024 * 1024 * 8 {
        todo!("Deny uploads larger than our max chunk size");
    }

    let storage_usages = meta_db.total_usages(&[user_id]).await.unwrap();
    let storage_usage = *storage_usages.get(&user_id).unwrap();

    let storage_caps = meta_db.storage_caps(&[user_id]).await.unwrap();
    let storage_cap = *storage_caps.get(&user_id).unwrap();

    event!(
        Level::INFO,
        storage_usage = storage_usage,
        storage_cap = storage_cap,
        chunk_size = chunk.len(),
    );

    if storage_usage + chunk.len() as u64 > storage_cap {
        todo!("Deny uploads that exceed storage cap");
    }

    let chunk_id = ChunkID::try_from(enc_chunk_metadata.id.as_str()).unwrap();

    let meta_db = meta_db.clone();
    let chunk_db = chunk_db.clone();

    chunk_db
        .put_chunk(&chunk_id, user_id, chunk.as_slice())
        .await
        .unwrap();

    meta_db
        .insert_enc_chunk_meta(enc_chunk_metadata, chunk.len().try_into().unwrap(), user_id)
        .await
        .unwrap();

    Ok(())
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_delete_chunks<D: MetaDB, C: ChunkDB>(
    meta_db: &D,
    chunk_db: &C,
    token: &Biscuit,
    chunk_ids: HashSet<ChunkID>,
) -> Result<()> {
    let user_id = authorize(Right::Delete, token, Vec::new(), meta_db)
        .await
        .unwrap();

    meta_db.delete_chunk_metas(&chunk_ids).await.unwrap();
    let remove_chunk_files = chunk_ids.clone().into_iter().map(|chunk_id| {
        async move {
            let chunk_id = chunk_id.clone();
            // TODO: delete multiple chunks at once
            chunk_db.delete_chunk(&chunk_id, user_id).await.unwrap();
        }
    });

    futures::future::join_all(remove_chunk_files).await;

    Ok(())
}

#[derive(Debug)]
pub enum UploadMetadataError {
    MultipleUserIDs,
}

impl Display for UploadMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UploadMetadataError::MultipleUserIDs => f.write_str("Multiple user ids"),
        }
    }
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_upload_file_metadata<D: MetaDB>(
    meta_db: &D,
    token: &Biscuit,
    enc_file_meta: EncryptedFileMetadata,
) -> Result<(), UploadMetadataError> {
    let user_id = authorize(Right::Write, token, Vec::new(), meta_db)
        .await
        .unwrap();

    let storage_usages = meta_db.total_usages(&[user_id]).await.unwrap();
    let storage_usage = *storage_usages.get(&user_id).unwrap();

    let storage_caps = meta_db.storage_caps(&[user_id]).await.unwrap();
    let storage_cap = *storage_caps.get(&user_id).unwrap();

    if storage_usage + enc_file_meta.metadata.len() as u64 > storage_cap {
        todo!("Deny uploads that exceed storage cap");
    }

    meta_db
        .insert_file_meta(enc_file_meta, user_id)
        .await
        .unwrap();

    Ok(())
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_download_file_metadata<D: MetaDB>(
    meta_db: &D,
    token: &Biscuit,
    file_id: String,
) -> Result<EncryptedFileMetadata, anyhow::Error> {
    let user_id = authorize(Right::Read, token, vec![file_id.clone()], meta_db)
        .await
        .unwrap();

    match meta_db.get_file_meta(file_id, user_id).await.unwrap() {
        Some(meta) => Ok(meta),
        None => Err(todo!()),
    }
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_list_file_metadata<D: MetaDB>(
    meta_db: &D,
    token: &Biscuit,
    file_ids: Vec<String>,
) -> Result<HashMap<String, EncryptedFileMetadata>, UploadMetadataError> {
    let user_id = authorize(Right::Query, token, file_ids.clone(), meta_db)
        .await
        .unwrap();
    let meta_ids: HashSet<String> = HashSet::from_iter(file_ids.into_iter());

    let meta = meta_db.list_file_meta(meta_ids, user_id).await.unwrap();
    Ok(meta)
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_list_chunk_metadata<D: MetaDB>(
    meta_db: &D,
    token: &Biscuit,
    chunk_ids: HashSet<ChunkID>,
) -> Result<HashMap<ChunkID, ChunkMetadata>, UploadMetadataError> {
    let user_id = authorize(Right::Query, token, Vec::new(), meta_db)
        .await
        .unwrap();

    let meta = meta_db.list_chunk_meta(chunk_ids, user_id).await.unwrap();
    Ok(meta)
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_delete_file_metadata<D: MetaDB>(
    meta_db: &D,
    token: &Biscuit,
    file_id: String,
) -> Result<(), UploadMetadataError> {
    let user_id = authorize(Right::Delete, token, vec![file_id.clone()], meta_db)
        .await
        .unwrap();

    meta_db.delete_file_meta(file_id, user_id).await.unwrap();

    Ok(())
}

#[tracing::instrument(err, skip(token))]
pub async fn handle_get_usage<D: MetaDB>(meta_db: &D, token: &Biscuit) -> anyhow::Result<u64> {
    let user_id = authorize(Right::Usage, token, Vec::new(), meta_db)
        .await
        .unwrap();

    Ok(*meta_db
        .total_usages(&[user_id])
        .await?
        .get(&user_id)
        .unwrap())
}
