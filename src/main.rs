// TODO: StorageBackendTrait
mod db;

use std::{
    collections::HashMap,
    os::unix::prelude::MetadataExt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use bfsp::{
    auth::{Authentication, ExpirationCaveat, UsernameCaveat},
    Action, ChunkID, ChunkMetadata, ChunksUploaded, ChunksUploadedQuery, DownloadChunkReq,
};
use log::{debug, info, trace};
use macaroon::{ByteString, Caveat, Macaroon, MacaroonKey, Verifier};
use rkyv::Deserialize;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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
        let db = db.clone();

        tokio::task::spawn(async move {
            loop {
                let action = match sock.read_u16().await {
                    Ok(action) => action,
                    Err(err) => {
                        trace!("Disconnecting due to error: {err:?}");
                        break;
                    }
                };
                let action: Action = action
                    .try_into()
                    .map_err(|_| anyhow!("Invalid action {action}"))
                    .unwrap();

                match action {
                    Action::UploadChunk => {
                        handle_upload_chunk(&mut sock, db.as_ref(), &macaroon_key)
                            .await
                            .unwrap()
                    }
                    Action::QueryChunksUploaded => {
                        query_chunks_uploaded(&mut sock, db.as_ref(), &macaroon_key)
                            .await
                            .unwrap()
                    }
                    Action::DownloadChunk => {
                        handle_download_chunk(&mut sock, db.as_ref(), &macaroon_key)
                            .await
                            .unwrap()
                    }
                };
            }

            debug!("Disconnecting from {addr}");
        });
    }

    Ok(())
}

pub async fn handle_download_chunk<D: ChunkDatabase>(
    sock: &mut TcpStream,
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
) -> Result<()> {
    let req_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; req_len];
    sock.read_exact(&mut buf).await?;

    let auth = rkyv::check_archived_root::<Authentication>(&buf)
        .map_err(|_| anyhow!("could not deserialize authentication"))?;

    let macaroon = Macaroon::deserialize(auth.macaroon.as_str())?;
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
    verifier.satisfy_general(check_token_expired);

    verifier.verify(&macaroon, macaroon_key, Vec::new())?;

    debug!("Handling chunk request");
    trace!("Getting request length");
    let req_len = sock.read_u16().await? as usize;

    let mut buf = vec![0; req_len];
    trace!("Reading chunk request");
    sock.read_exact(&mut buf).await?;

    let req = rkyv::check_archived_root::<DownloadChunkReq>(&buf)
        .map_err(|_| anyhow!("Could not deserialize download request"))?;
    let req: DownloadChunkReq = req.deserialize(&mut rkyv::Infallible).unwrap();

    let path = format!("chunks/{}", req.chunk_id);

    let chunk_meta = chunk_db
        .get_chunk_meta(req.chunk_id, username_caveat.username.as_str())
        .await?
        .ok_or_else(|| anyhow!("chunk not found"))?;
    let chunk_meta_bytes = chunk_meta.to_bytes()?;
    sock.write_all(&chunk_meta_bytes).await?;

    let mut chunk_file = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&path)
        .await?;

    let chunk_file_metadata = fs::metadata(path).await?;

    trace!("Sending chunk");

    sock.write_u32(chunk_file_metadata.size() as u32).await?;
    tokio::io::copy(&mut chunk_file, sock).await?;

    Ok(())
}

// TODO: very ddosable by querying many chunks at once
async fn query_chunks_uploaded<D: ChunkDatabase>(
    sock: &mut TcpStream,
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
) -> Result<()> {
    let req_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; req_len];
    sock.read_exact(&mut buf).await?;

    let auth = rkyv::check_archived_root::<Authentication>(&buf)
        .map_err(|_| anyhow!("could not deserialize authentication"))?;

    let macaroon = Macaroon::deserialize(auth.macaroon.as_str())?;
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

    verifier.verify(&macaroon, macaroon_key, Vec::new())?;

    let chunks_uploaded_query_len: u16 = sock.read_u16().await?;
    let mut chunks_uploaded_query_bin = vec![0; chunks_uploaded_query_len as usize];

    sock.read_exact(&mut chunks_uploaded_query_bin).await?;

    let chunks_uploaded_query =
        rkyv::check_archived_root::<ChunksUploadedQuery>(&chunks_uploaded_query_bin)
            .map_err(|_| anyhow!("Error deserializing ChunksUploadedQuery"))?;
    let chunks_uploaded_query: ChunksUploadedQuery =
        chunks_uploaded_query.deserialize(&mut rkyv::Infallible)?;

    let username = &username_caveat.username;

    let chunks_uploaded: HashMap<ChunkID, bool> = futures::future::join_all(
        chunks_uploaded_query
            .chunks
            .iter()
            .map(|chunk_id| async move {
                let chunk_id: ChunkID = *chunk_id;
                let contains_chunk: bool =
                    chunk_db.contains_chunk(chunk_id, username).await.unwrap();

                (chunk_id, contains_chunk)
            }),
    )
    .await
    .into_iter()
    .collect();

    let chunks_uploaded = ChunksUploaded {
        chunks: chunks_uploaded,
    };

    sock.write_all(&chunks_uploaded.to_bytes()?).await?;

    Ok(())
}

async fn handle_upload_chunk<D: ChunkDatabase>(
    sock: &mut TcpStream,
    chunk_db: &D,
    macaroon_key: &MacaroonKey,
) -> Result<()> {
    trace!("Handling chunk upload");
    let req_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; req_len];
    sock.read_exact(&mut buf).await?;

    let auth = rkyv::check_archived_root::<Authentication>(&buf)
        .map_err(|_| anyhow!("could not deserialize authentication"))?;

    let macaroon = Macaroon::deserialize(auth.macaroon.as_str())?;
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

    verifier.satisfy_general(check_token_expired);
    verifier.satisfy_exact(username_caveat.clone().into());
    verifier.verify(&macaroon, macaroon_key, Vec::new())?;

    let chunk_metadata_len = sock.read_u16().await? as usize;
    let mut chunk_metadata_buf = vec![0; chunk_metadata_len];
    sock.read_exact(&mut chunk_metadata_buf[..chunk_metadata_len])
        .await?;

    let chunk_metadata =
        rkyv::check_archived_root::<ChunkMetadata>(&chunk_metadata_buf[..chunk_metadata_len])
            .map_err(|_| anyhow!("Error deserializing chunk metadata"))?;
    let chunk_metadata: ChunkMetadata = chunk_metadata.deserialize(&mut rkyv::Infallible)?;

    // 8MiB(?)
    if chunk_metadata.size > 1024 * 1024 * 8 {
        todo!("Deny uploads larger than our max chunk size");
    }

    let chunk_id = &chunk_metadata.id;
    let mut chunk_file = fs::File::create(format!("./chunks/{}", chunk_id)).await?;

    let expected_size = sock.read_u32().await?;

    let mut chunk_sock = sock.take(expected_size.into());
    let bytes_copied = tokio::io::copy(&mut chunk_sock, &mut chunk_file).await;

    match bytes_copied {
        Ok(bytes_copied) => {
            // The client lied on how much it would copy :(, delete the chunk
            if bytes_copied != expected_size as u64 {
                fs::remove_file(format!("./chunks/{chunk_id}")).await?;
                return Err(anyhow!(
                    "Expected {} bytes to chunk {chunk_id}, got {bytes_copied}",
                    chunk_metadata.size
                ));
            }
        }
        Err(err) => {
            fs::remove_file(format!("./chunks/{chunk_id}")).await?;
            return Err(err.into());
        }
    }

    chunk_db
        .insert_chunk(chunk_metadata, &username_caveat.username)
        .await?;

    Ok(())
}

fn check_token_expired(caveat: &ByteString) -> bool {
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
