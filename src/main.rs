// TODO: StorageBackendTrait
mod db;

use anyhow::anyhow;
use bfsp::list_file_metadata_resp::FileMetadatas;
use biscuit_auth::{macros::authorizer, Authorizer, Biscuit, KeyPair, PrivateKey};
use std::fmt::Display;
use std::{
    collections::{HashMap, HashSet},
    os::unix::prelude::MetadataExt,
    sync::Arc,
};

use anyhow::Result;
use bfsp::{
    chunks_uploaded_query_resp::{ChunkUploaded, ChunksUploaded},
    download_chunk_resp::ChunkData,
    file_server_message::Message::{
        ChunksUploadedQuery, DeleteChunksQuery, DownloadChunkQuery, DownloadFileMetadataQuery,
        ListFileMetadataQuery, UploadChunk, UploadFileMetadata,
    },
    ChunkID, ChunkMetadata, ChunksUploadedQueryResp, DownloadChunkResp, FileServerMessage, Message,
};
use bfsp::{EncryptedFileMetadata, PrependLen};
use log::{debug, info, trace};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::db::{ChunkDatabase, InsertChunkError, SqliteDB};

#[tokio::main]
async fn main() -> Result<()> {
    fern::Dispatch::new()
        .format(|out, msg, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                humantime::format_rfc3339(std::time::SystemTime::now()),
                record.level(),
                record.target(),
                msg
            ))
        }) // Add blanket level filter -
        .level(log::LevelFilter::Trace)
        .level_for("sqlx", log::LevelFilter::Warn)
        // - and per-module overrides
        // Output to stdout, files, and other Dispatch configurations
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log")?)
        // Apply globally
        .apply()?;

    fs::create_dir_all("./chunks/").await?;

    let keys = fs::read_to_string("./.keys.json").await?;
    let keys: serde_json::Value = serde_json::from_str(&keys)?;
    let private_key = keys["private"].as_str().unwrap();
    let private_key = PrivateKey::from_bytes_hex(private_key)?;

    let root_key = KeyPair::from(&private_key);

    debug!("Initializing database");
    let db = Arc::new(SqliteDB::new().await.unwrap());

    info!("Starting server!");
    let sock = TcpListener::bind(":::9999").await.unwrap();

    let public_key = root_key.public();

    while let Ok((mut sock, addr)) = sock.accept().await {
        let db = Arc::clone(&db);

        tokio::task::spawn(async move {
            loop {
                let action_len = if let Ok(len) = sock.read_u32_le().await {
                    len
                } else {
                    info!("Disconnecting from {addr}");
                    return;
                };

                debug!("Action is {action_len} bytes");

                // 9 MiB
                if action_len > 9_437_184 {
                    todo!("Action {action_len} too big :(");
                }

                let command = {
                    let mut action_buf = vec![0; action_len as usize];
                    sock.read_exact(&mut action_buf).await.unwrap();
                    FileServerMessage::from_bytes(&action_buf).unwrap()
                };
                let authentication = command.auth.unwrap();
                debug!("{}", authentication.token);

                let token = Biscuit::from_base64(&authentication.token, public_key).unwrap();

                let resp: Vec<u8> = match command.message.unwrap() {
                    DownloadChunkQuery(query) => {
                        match handle_download_chunk(
                            db.as_ref(),
                            &token,
                            ChunkID::from_bytes(query.chunk_id.try_into().unwrap()),
                        )
                        .await
                        {
                            Ok(Some((meta, data))) => DownloadChunkResp {
                                response: Some(bfsp::download_chunk_resp::Response::ChunkData(
                                    ChunkData {
                                        chunk_metadata: Some(meta),
                                        chunk: data,
                                    },
                                )),
                            }
                            .encode_to_vec(),
                            Ok(None) => DownloadChunkResp {
                                response: Some(bfsp::download_chunk_resp::Response::Err(
                                    "ChunkNotFound".to_string(),
                                )),
                            }
                            .encode_to_vec(),
                            Err(_) => todo!(),
                        }
                    }
                    ChunksUploadedQuery(query) => {
                        let chunk_ids = query
                            .chunk_ids
                            .into_iter()
                            .map(|chunk_id| ChunkID::from_bytes(chunk_id.try_into().unwrap()))
                            .collect();
                        match query_chunks_uploaded(db.as_ref(), &token, chunk_ids).await {
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
                        let chunk_metadata = msg.chunk_metadata.unwrap();
                        let chunk = msg.chunk;

                        match handle_upload_chunk(db.as_ref(), &token, chunk_metadata, &chunk).await
                        {
                            Ok(_) => bfsp::UploadChunkResp { err: None }.encode_to_vec(),
                            Err(err) => todo!("{err}"),
                        }
                    }
                    DeleteChunksQuery(query) => {
                        let chunk_ids: HashSet<ChunkID> = query
                            .chunk_ids
                            .into_iter()
                            .map(|chunk_id| ChunkID::from_bytes(chunk_id.try_into().unwrap()))
                            .collect();

                        match handle_delete_chunks(db.as_ref(), &token, chunk_ids).await {
                            Ok(_) => bfsp::DeleteChunksResp { err: None }.encode_to_vec(),
                            Err(err) => todo!("{err}"),
                        }
                    }
                    UploadFileMetadata(meta) => {
                        let encrypted_file_meta = meta.encrypted_file_metadata.unwrap();
                        match handle_upload_metadata(db.as_ref(), &token, encrypted_file_meta).await
                        {
                            Ok(_) => bfsp::UploadFileMetadataResp { err: None }.encode_to_vec(),
                            Err(err) => todo!("{err:?}"),
                        }
                    }
                    DownloadFileMetadataQuery(query) => {
                        let meta_id = query.id;
                        match handle_download_file_metadata(db.as_ref(), &token, meta_id).await {
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
                        match handle_list_metadata(db.as_ref(), &token, meta_ids).await {
                            Ok(metas) => bfsp::ListFileMetadataResp {
                                response: Some(bfsp::list_file_metadata_resp::Response::Metadatas(
                                    FileMetadatas { metadatas: metas },
                                )),
                            }
                            .encode_to_vec(),
                            Err(_) => todo!(),
                        }
                    }
                }
                .prepend_len();

                debug!("Sent response");
                sock.write_all(&resp).await.unwrap();
            }
        });
    }

    Ok(())
}

pub async fn handle_download_chunk<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    chunk_id: ChunkID,
) -> Result<Option<(ChunkMetadata, Vec<u8>)>> {
    let mut authorizer = authorizer!(
        r#"
            check if user($user);
            check if rights($rights), $rights.contains("download");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let user_id = get_user_id(&mut authorizer).unwrap();

    let path = format!("chunks/{}", chunk_id);

    let chunk_meta = if let Some(chunk_meta) = chunk_db.get_chunk_meta(chunk_id, user_id).await? {
        chunk_meta
    } else {
        return Ok(None);
    };

    let mut chunk_file = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&path)
        .await?;

    let chunk_file_metadata = fs::metadata(path).await?;

    let mut chunk = Vec::with_capacity(chunk_file_metadata.size() as usize);
    chunk_file.read_to_end(&mut chunk).await?;

    Ok(Some((chunk_meta, chunk)))
}

// FIXME: very ddosable by querying many chunks at once
async fn query_chunks_uploaded<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    chunks: HashSet<ChunkID>,
) -> Result<HashMap<ChunkID, bool>> {
    let mut authorizer = authorizer!(
        r#"
            check if email($email);
            check if rights($rights), $rights.contains("query");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let user_id = get_user_id(&mut authorizer).unwrap();

    let chunks_uploaded: HashMap<ChunkID, bool> =
        futures::future::join_all(chunks.into_iter().map(|chunk_id| async move {
            let contains_chunk: bool = chunk_db.contains_chunk(chunk_id, user_id).await.unwrap();
            (chunk_id, contains_chunk)
        }))
        .await
        .into_iter()
        .collect();

    Ok(chunks_uploaded)
}

// TODO: Maybe store upload_chunk messages in files and mmap them?
async fn handle_upload_chunk<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    chunk_metadata: ChunkMetadata,
    chunk: &[u8],
) -> Result<()> {
    trace!("Handling chunk upload");

    let mut authorizer = authorizer!(
        r#"
            check if email($email);
            check if rights($rights), $rights.contains("upload");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let user_id = get_user_id(&mut authorizer).unwrap();

    // 8MiB(?)
    if chunk_metadata.size > 1024 * 1024 * 8 {
        todo!("Deny uploads larger than our max chunk size");
    }

    let chunk_id = ChunkID::from_bytes(
        chunk_metadata
            .id
            .clone()
            .try_into()
            .map_err(|_| anyhow!("Error deserializing ChunkID"))?,
    );
    trace!("Got chunk id");

    if let Err(err) = chunk_db.insert_chunk(chunk_metadata, user_id).await {
        if let InsertChunkError::AlreadyExists = err {
            // If the chunk already exists, no point in re-uploading it. Just tell the user we processed it :)
            return Ok(());
        } else {
            return Err(err.into());
        }
    };
    trace!("Inserting chunk into db");
    info!("Uploaded chunk {chunk_id}");

    let mut chunk_file = fs::File::create(format!("./chunks/{}", chunk_id)).await?;
    trace!("Created chunk file");

    chunk_file.write_all(chunk).await?;
    trace!("Wrote chunk file");

    Ok(())
}

pub async fn handle_delete_chunks<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    chunk_ids: HashSet<ChunkID>,
) -> Result<()> {
    trace!("Handling delete chunk");

    let mut authorizer = authorizer!(
        r#"
            check if email($email);
            check if rights($rights), $rights.contains("delete");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let remove_chunk_files = chunk_ids.clone().into_iter().map(|chunk_id| async move {
        let path = format!("./chunks/{chunk_id}");
        fs::remove_file(path).await.unwrap();
    });

    tokio::join!(
        async move {
            futures::future::join_all(remove_chunk_files).await;
        },
        async move {
            chunk_db.delete_chunks(&chunk_ids).await.unwrap();
        },
    );

    Ok(())
}

#[derive(Debug)]
pub enum UploadMetadataError {
    MultipleUserIDs,
}

pub async fn handle_upload_metadata<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    enc_file_meta: EncryptedFileMetadata,
) -> Result<(), UploadMetadataError> {
    let mut authorizer = authorizer!(
        r#"
            check if user($user);
            check if rights($rights), $rights.contains("delete");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let user_id = get_user_id(&mut authorizer).unwrap();
    chunk_db
        .insert_file_meta(enc_file_meta, user_id)
        .await
        .unwrap();

    Ok(())
}

pub async fn handle_download_file_metadata<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    meta_id: i64,
) -> Result<EncryptedFileMetadata, UploadMetadataError> {
    let mut authorizer = authorizer!(
        r#"
            check if user($user);
            check if rights($rights), $rights.contains("download");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let user_id = get_user_id(&mut authorizer).unwrap();
    match chunk_db.get_file_meta(meta_id, user_id).await.unwrap() {
        Some(meta) => Ok(meta),
        None => Err(todo!()),
    }
}

pub async fn handle_list_metadata<D: ChunkDatabase>(
    chunk_db: &D,
    token: &Biscuit,
    meta_ids: Vec<i64>,
) -> Result<HashMap<i64, EncryptedFileMetadata>, UploadMetadataError> {
    let mut authorizer = authorizer!(
        r#"
            check if user($user);
            check if rights($rights), $rights.contains("query");
            allow if true;
            deny if false;
        "#
    );

    authorizer.add_token(token).unwrap();
    authorizer.authorize().unwrap();

    let meta_ids: HashSet<i64> = HashSet::from_iter(meta_ids.into_iter());

    let user_id = get_user_id(&mut authorizer).unwrap();
    let meta = chunk_db.list_file_meta(meta_ids, user_id).await.unwrap();
    Ok(meta)
}

#[derive(thiserror::Error, Debug)]
pub enum GetUserIDError {
    MultipleUserIDs,
}

impl Display for GetUserIDError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("multipe user ids")
    }
}

pub fn get_user_id(authorizer: &mut Authorizer) -> Result<i64, GetUserIDError> {
    let user_info: Vec<(String,)> = authorizer.query("data($0) <- user($0)").unwrap();

    debug!("{user_info:#?}");

    if user_info.len() != 1 {
        return Err(GetUserIDError::MultipleUserIDs);
    }

    let user_id: i64 = user_info.first().unwrap().0.parse().unwrap();
    Ok(user_id)
}
