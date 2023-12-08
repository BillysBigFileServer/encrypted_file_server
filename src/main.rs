// TODO: StorageBackendTrait
mod db;

use anyhow::anyhow;
use std::{
    collections::{HashMap, HashSet},
    os::unix::prelude::MetadataExt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use bfsp::PrependLen;
use bfsp::{
    auth::{EmailCaveat, ExpirationCaveat},
    chunks_uploaded_query_resp::{ChunkUploaded, ChunksUploaded},
    download_chunk_resp::ChunkData,
    file_server_message::Message::{ChunksUploadedQuery, DownloadChunkQuery, UploadChunkQuery},
    AuthErr, ChunkID, ChunkMetadata, ChunksUploadedQueryResp, DownloadChunkResp, FileServerMessage,
    Message,
};
use log::{debug, info, trace};
use macaroon::{ByteString, Caveat, Macaroon, MacaroonKey, Verifier};
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

    debug!("Initializing database");
    let db = Arc::new(SqliteDB::new().await.unwrap());

    info!("Starting server!");
    let sock = TcpListener::bind(":::9999").await.unwrap();
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
                let macaroon = Macaroon::deserialize(&authentication.macaroon).unwrap();

                let resp: Vec<u8> = match command.message.unwrap() {
                    DownloadChunkQuery(query) => {
                        match handle_download_chunk(
                            db.as_ref(),
                            &macaroon_key,
                            macaroon,
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
                        match query_chunks_uploaded(db.as_ref(), &macaroon_key, macaroon, chunk_ids)
                            .await
                        {
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
                            Err(_) => todo!(),
                        }
                    }
                    UploadChunkQuery(query) => {
                        let chunk_metadata = query.chunk_metadata.unwrap();
                        let chunk = query.chunk;

                        match handle_upload_chunk(
                            db.as_ref(),
                            &macaroon_key,
                            macaroon,
                            chunk_metadata,
                            &chunk,
                        )
                        .await
                        {
                            Ok(_) => bfsp::UploadChunkResp { err: None }.encode_to_vec(),
                            Err(err) => todo!("{err}"),
                        }
                    }
                }
                .prepend_len();

                sock.write_all(&resp).await.unwrap();
            }
        });
    }

    Ok(())
}

pub async fn handle_download_chunk<D: ChunkDatabase>(
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
    macaroon: Macaroon,
    chunk_id: ChunkID,
) -> Result<Option<(ChunkMetadata, Vec<u8>)>> {
    let caveats = macaroon.first_party_caveats();
    let email = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };

            let email_caveat: EmailCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(email_caveat.email)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_exact(format!("email = {email}").into());
    verifier.satisfy_general(check_token_valid);

    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|_| AuthErr)?;

    let path = format!("chunks/{}", chunk_id);

    let chunk_meta = if let Some(chunk_meta) = chunk_db.get_chunk_meta(chunk_id, &email).await? {
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
    macaroon_key: &MacaroonKey,
    macaroon: Macaroon,
    chunks: HashSet<ChunkID>,
) -> Result<HashMap<ChunkID, bool>> {
    let caveats = macaroon.first_party_caveats();

    let email_caveat = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };
            let email_caveat: EmailCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(email_caveat)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_exact(email_caveat.clone().into());
    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|_| AuthErr)?;

    let email = &email_caveat.email;

    let chunks_uploaded: HashMap<ChunkID, bool> =
        futures::future::join_all(chunks.into_iter().map(|chunk_id| async move {
            let contains_chunk: bool = chunk_db.contains_chunk(chunk_id, email).await.unwrap();
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
    let email = caveats
        .iter()
        .find_map(|caveat| {
            let Caveat::FirstParty(caveat) = caveat else {
                return None;
            };

            let email_caveat: EmailCaveat = match caveat.predicate().try_into() {
                Ok(caveat) => caveat,
                Err(_) => return None,
            };

            Some(email_caveat.email)
        })
        .unwrap();

    let mut verifier = Verifier::default();

    verifier.satisfy_general(check_token_valid);
    //FIXME:
    /*
    verifier.satisfy_exact(format!("email = {email}").into());
    verifier
        .verify(&macaroon, macaroon_key, Vec::new())
        .map_err(|err| {
            debug!("Error verifying macaroon: {err}");
            AuthErr
        })?;
        */

    trace!("Verified caveats");

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

    let mut chunk_file = fs::File::create(format!("./chunks/{}", chunk_id)).await?;
    trace!("Created chunk file");

    chunk_file.write_all(chunk).await?;
    trace!("Wrote chunk file");

    chunk_db.insert_chunk(chunk_metadata, &email).await?;
    trace!("Inserting chunk into db");
    info!("Uploaded chunk {chunk_id}");

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
