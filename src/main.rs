mod auth;

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bfsp::{Action, ChunkID, ChunkMetadata, ChunksUploaded, ChunksUploadedQuery, DownloadChunkReq};
use dashmap::{DashMap, DashSet};
use log::{debug, info, trace};
use once_cell::sync::Lazy;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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

    fs::create_dir_all("./files/tmp").await?;
    fs::create_dir_all("./chunks/tmp").await?;

    info!("Starting server!");

    let sock = TcpListener::bind(":::9999").await.unwrap();
    while let Ok((mut sock, addr)) = sock.accept().await {
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
                    Action::UploadChunk => handle_upload_chunk(&mut sock).await.unwrap(),
                    Action::QueryChunksUploaded => query_chunks_uploaded(&mut sock).await.unwrap(),
                    Action::DownloadChunk => handle_download_chunk(&mut sock).await.unwrap(),
                };
            }

            debug!("Disconnecting from {addr}");
        });
    }

    Ok(())
}

pub async fn handle_download_chunk(sock: &mut TcpStream) -> Result<()> {
    let req_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; req_len];
    sock.read_exact(&mut buf).await?;
    let req = rkyv::check_archived_root::<DownloadChunkReq>(&buf)
        .map_err(|_| anyhow!("Could not deserialize download request"))?;

    let path = format!("chunks/{}", req.chunk_id);

    let mut chunk_file = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(path)
        .await?;
    tokio::io::copy(&mut chunk_file, sock).await?;

    Ok(())
}

// TODO: very ddosable by querying many chunks at once
async fn query_chunks_uploaded(sock: &mut TcpStream) -> Result<()> {
    let chunks_uploaded_query_len: u16 = sock.read_u16().await?;
    let chunks_uploaded_query_bin = vec![0; chunks_uploaded_query_len as usize];

    let chunks_uploaded_query =
        rkyv::check_archived_root::<ChunksUploadedQuery>(&chunks_uploaded_query_bin)
            .map_err(|_| anyhow!("Error deserializing chunk metadata"))?;

    //TODO: parallelize
    let chunks_uploaded: HashMap<ChunkID, bool> = chunks_uploaded_query
        .chunks
        .iter()
        .map(|chunk_id| {
            let chunk_id: ChunkID = chunk_id.into();
            (chunk_id, CHUNKS_UPLOADED.contains(&chunk_id))
        })
        .collect();

    let chunks_uploaded = ChunksUploaded {
        chunks: chunks_uploaded,
    };

    sock.write_all(&chunks_uploaded.to_bytes()?).await?;

    Ok(())
}

static CHUNKS_UPLOADED: Lazy<DashSet<ChunkID>> = Lazy::new(DashSet::new);

async fn handle_upload_chunk(sock: &mut TcpStream) -> Result<()> {
    trace!("Handling chunk upload");
    let chunk_metadata_len = sock.read_u16().await? as usize;
    let mut chunk_metadata_buf = vec![0; chunk_metadata_len];
    sock.read_exact(&mut chunk_metadata_buf[..chunk_metadata_len])
        .await?;

    let chunk_metadata =
        rkyv::check_archived_root::<ChunkMetadata>(&chunk_metadata_buf[..chunk_metadata_len])
            .map_err(|_| anyhow!("Error deserializing chunk metadata"))?;

    // 8MiB(?)
    if chunk_metadata.size > 1024 * 1024 * 8 {
        todo!("Deny uploads larger than our max chunk size");
    }

    let chunk_id = &chunk_metadata.id;
    let mut chunk_file = fs::File::create(format!("./chunks/{}", chunk_id)).await?;

    let mut chunk_sock = sock.take(chunk_metadata.size.try_into().unwrap());
    let bytes_copied = tokio::io::copy(&mut chunk_sock, &mut chunk_file).await;

    match bytes_copied {
        Ok(bytes_copied) => {
            // The client lied on how much it would copy :(, delete the chunk
            if bytes_copied != chunk_metadata.size as u64 {
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

    Ok(())
}
