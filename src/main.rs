// TODO: StorageBackendTrait
mod db;

use std::{
    collections::{HashMap, HashSet},
    os::unix::prelude::MetadataExt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use bfsp::{
    auth::{ExpirationCaveat, UsernameCaveat},
    AuthErr, ChunkData, ChunkID, ChunkMetadata, ChunkNotFound, ChunksUploadedErr,
    ChunksUploadedQueryResp, DownloadChunkErr, DownloadChunkResp, FileServerMessage,
    UploadChunkErr, UploadChunkResp,
};
use log::{debug, info, trace};
use macaroon::{ByteString, Caveat, Macaroon, MacaroonKey, Verifier};
use rkyv::{collections::ArchivedHashSet, Deserialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::db::{ChunkDatabase, SqliteDB};

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

    let macaroon_key = MacaroonKey::generate(b"key");

    info!("Starting server!");

    debug!("Initializing database");
    let db = Arc::new(SqliteDB::new().await.unwrap());

    let sock = TcpListener::bind(":::9999").await.unwrap();
    while let Ok((mut sock, addr)) = sock.accept().await {
        let db = Arc::clone(&db);
        tokio::task::spawn(async move {
            loop {
                let action_len = if let Ok(len) = sock.read_u32().await {
                    len
                } else {
                    return;
                };

                // 9 MiB
                if action_len > 9_437_184 {
                    todo!("Action too big :(");
                }

                let mut action_buf = vec![0; action_len as usize];
                sock.read_exact(&mut action_buf).await.unwrap();

                let command = rkyv::check_archived_root::<FileServerMessage>(&action_buf).unwrap();

                let macaroon = Macaroon::deserialize(command.auth.macaroon.as_str()).unwrap();

                let resp: Vec<u8> = match &command.action {
                    bfsp::ArchivedAction::UploadChunk {
                        chunk_metadata,
                        chunk,
                    } => match handle_upload_chunk(
                        db.as_ref(),
                        &macaroon_key,
                        macaroon,
                        chunk_metadata.deserialize(&mut rkyv::Infallible).unwrap(),
                        &chunk,
                    )
                    .await
                    {
                        Ok(()) => UploadChunkResp::Ok,
                        Err(err) => {
                            UploadChunkResp::Err(if err.downcast_ref::<AuthErr>().is_some() {
                                UploadChunkErr::AuthErr
                            } else {
                                UploadChunkErr::InternalServerErr
                            })
                        }
                    }
                    .to_bytes()
                    .unwrap()
                    .to_vec(),
                    bfsp::ArchivedAction::QueryChunksUploaded { chunks } => {
                        match query_chunks_uploaded(db.as_ref(), &macaroon_key, macaroon, chunks)
                            .await
                        {
                            Ok(chunks_uploaded) => ChunksUploadedQueryResp::ChunksUploaded {
                                chunks: chunks_uploaded,
                            },
                            Err(err) => ChunksUploadedQueryResp::Err(
                                if err.downcast_ref::<AuthErr>().is_some() {
                                    ChunksUploadedErr::AuthErr
                                } else {
                                    ChunksUploadedErr::InternalServerErrr
                                },
                            ),
                        }
                        .to_bytes()
                        .unwrap()
                        .to_vec()
                    }
                    bfsp::ArchivedAction::DownloadChunk { chunk_id } => {
                        match handle_download_chunk(
                            db.as_ref(),
                            &macaroon_key,
                            macaroon,
                            chunk_id.deserialize(&mut rkyv::Infallible).unwrap(),
                        )
                        .await
                        {
                            Ok(chunk_data) => DownloadChunkResp::ChunkData {
                                meta: chunk_data.meta,
                                chunk: chunk_data.chunk,
                            },
                            Err(err) => {
                                DownloadChunkResp::Err(if err.downcast_ref::<AuthErr>().is_some() {
                                    DownloadChunkErr::AuthErr
                                } else if err.downcast_ref::<ChunkNotFound>().is_some() {
                                    DownloadChunkErr::ChunkNotFound
                                } else {
                                    //warn!("Error sending chunk: {err}");
                                    DownloadChunkErr::InternalServerErr
                                })
                            }
                        }
                        .to_bytes()
                        .unwrap()
                        .to_vec()
                    }
                };

                sock.write_all(&resp).await.unwrap();
            }

            info!("Disconnecting from {addr}");
        });
    }

    Ok(())
}

pub async fn handle_download_chunk<D: ChunkDatabase>(
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
    macaroon: Macaroon,
    chunk_id: ChunkID,
) -> Result<ChunkData> {
    let caveats = macaroon.first_party_caveats();

    let username_caveat = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };
            let username_caveat: UsernameCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(username_caveat)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_exact(username_caveat.clone().into());
    verifier.satisfy_general(check_token_valid);

    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|_| AuthErr)?;

    let path = format!("chunks/{}", chunk_id);

    let chunk_meta = chunk_db
        .get_chunk_meta(chunk_id, username_caveat.username.as_str())
        .await?
        .ok_or_else(|| ChunkNotFound)?;

    let mut chunk_file = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&path)
        .await?;

    let chunk_file_metadata = fs::metadata(path).await?;

    let mut chunk = Vec::with_capacity(chunk_file_metadata.size() as usize);
    chunk_file.read_to_end(&mut chunk).await?;

    Ok(ChunkData {
        meta: chunk_meta,
        chunk,
    })
}

// FIXME: very ddosable by querying many chunks at once
async fn query_chunks_uploaded<D: ChunkDatabase>(
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
    macaroon: Macaroon,
    chunks: &ArchivedHashSet<bfsp::ArchivedChunkID>,
) -> Result<HashMap<ChunkID, bool>> {
    let caveats = macaroon.first_party_caveats();

    let username_caveat = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };
            let username_caveat: UsernameCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(username_caveat)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_exact(username_caveat.clone().into());
    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|_| AuthErr)?;

    let username = &username_caveat.username;

    let chunks: HashSet<ChunkID> = chunks.deserialize(&mut rkyv::Infallible)?;
    let chunks_uploaded: HashMap<ChunkID, bool> =
        futures::future::join_all(chunks.into_iter().map(|chunk_id| async move {
            let contains_chunk: bool = chunk_db.contains_chunk(chunk_id, username).await.unwrap();
            (chunk_id, contains_chunk)
        }))
        .await
        .into_iter()
        .collect();

    Ok(chunks_uploaded)
}

async fn handle_upload_chunk<D: ChunkDatabase>(
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
    macaroon: Macaroon,
    chunk_metadata: ChunkMetadata,
    chunk: &[u8],
) -> Result<()> {
    trace!("Handling chunk upload");

    let caveats = macaroon.first_party_caveats();

    // TODO: swap this to satisfy_general
    let username_caveat = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };
            let username_caveat: UsernameCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(username_caveat)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_general(check_token_valid);
    verifier.satisfy_exact(username_caveat.clone().into());
    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|_| AuthErr)?;

    // 8MiB(?)
    if chunk_metadata.size > 1024 * 1024 * 8 {
        todo!("Deny uploads larger than our max chunk size");
    }

    let chunk_id = &chunk_metadata.id;
    let mut chunk_file = fs::File::create(format!("./chunks/{}", chunk_id)).await?;

    chunk_file.write_all(chunk).await?;

    chunk_db
        .insert_chunk(chunk_metadata, &username_caveat.username)
        .await?;

    Ok(())
}

fn check_token_valid(caveat: &ByteString) -> bool {
    let caveat: ExpirationCaveat = match caveat.try_into() {
        Ok(caveat) => caveat,
        Err(_) => return false,
    };
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    current_time > caveat.expiration
}
