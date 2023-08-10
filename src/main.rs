mod auth;

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bfsp::*;
use blake3::{Hash, Hasher};
use dashmap::DashMap;
use faster_hex::hex_string;
use log::{debug, info, trace};
use once_cell::sync::Lazy;
use rkyv::Deserialize;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
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

    info!("Starting server!");

    let sock = TcpListener::bind(":::9999").await.unwrap();
    while let Ok((mut sock, _addr)) = sock.accept().await {
        tokio::task::spawn(async move {
            loop {
                let action = match sock.read_u16().await {
                    Ok(action) => action,
                    Err(_) => break,
                };
                let action: Action = action
                    .try_into()
                    .map_err(|_| anyhow!("Invalid action {action}"))
                    .unwrap();

                match action {
                    Action::Upload => handle_upload(&mut sock).await.unwrap(),
                    Action::QueryPartialUpload => query_partial_upload(&mut sock).await.unwrap(),
                    Action::Download => handle_download(&mut sock).await.unwrap(),
                };
            }
        });
    }

    Ok(())
}

pub async fn handle_download(sock: &mut TcpStream) -> Result<()> {
    let req_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; req_len];
    sock.read_exact(&mut buf).await?;
    let req = rkyv::check_archived_root::<DownloadReq>(&buf)
        .map_err(|_| anyhow!("Could not deserialize download request"))?;

    let (file_header, chunks_to_write) =
        match CHUNKS_WRITTEN.get(&blake3::Hash::from_bytes(req.file_hash)) {
            Some(c) => {
                let c = c.clone();

                (
                    c.0.clone(),
                    c.1.into_iter()
                        .filter_map(|(chunk_id, chunk_written)| match chunk_written {
                            true => Some(chunk_id),
                            false => None,
                        }),
                )
            }
            None => todo!(),
        };

    let mut file = fs::File::open(format!("./files/{}", hex_string(&file_header.hash))).await?;
    sock.write_all(&file_header.to_bytes()?).await?;
    for chunk_id in chunks_to_write {
        let chunk_metadata = file_header.chunks.get(&chunk_id).unwrap();
        sock.write_all(&chunk_metadata.to_bytes()?).await?;

        let chunk = chunk_from_file(&file_header, &mut file, chunk_id).await?;
        sock.write_all(&chunk).await?;
    }

    Ok(())
}

async fn query_partial_upload(sock: &mut TcpStream) -> Result<()> {
    let mut hash = [0; blake3::OUT_LEN];
    sock.read_exact(&mut hash).await?;

    let hash = Hash::from_bytes(hash);
    let chunks = CHUNKS_WRITTEN
        .get(&hash)
        .map(|chunks_written| chunks_written.1.clone());

    let partial = ChunksUploaded { chunks };

    sock.write_all(&partial.to_bytes()?).await?;

    Ok(())
}

pub static CHUNKS_WRITTEN: Lazy<DashMap<Hash, (FileHeader, HashMap<ChunkID, bool>)>> =
    Lazy::new(DashMap::new);

async fn handle_upload(sock: &mut TcpStream) -> Result<()> {
    // // FIXME: validate cookie
    // let cookie = String::from_utf8(buf[..blake3::OUT_LEN].to_vec())?;
    trace!("Handling upload");

    let mut file_header_buf = [0_u8; 1024];

    let file_header = {
        let file_header_len = sock.read_u16().await? as usize;
        sock.read_exact(&mut file_header_buf[..file_header_len])
            .await?;
        let file_header_bytes = &file_header_buf[..file_header_len];

        rkyv::check_archived_root::<FileHeader>(file_header_bytes)
            .map_err(|_| anyhow!("Error deserializing file header"))?
    };

    trace!(
        "Received file header with hash: {}",
        hex_string(&file_header.hash)
    );

    // First, create the temporary file
    let tmp_file_path = format!("./files/tmp/{}", &hex_string(&file_header.hash));
    let mut tmp_file = tokio::fs::File::create(&tmp_file_path).await?;
    // Next, fill it with zeroes
    tmp_file.set_len(file_header.total_file_size()).await?;

    trace!("Setup upload file {}", hex_string(&file_header.hash));

    // FIXME: Have a maximum chunk size
    let mut chunk_buf = vec![0; file_header.chunk_size as usize];
    let mut chunk_metadata_buf = [0; 1024];

    // Note: We don't check the final file hash of the file since, if all the chunk hashes match, so will the final hash
    let use_parallel_hasher = use_parallel_hasher(file_header.chunk_size as usize);
    let file_header_hash = Hash::from_bytes(file_header.hash);

    let chunks_being_uploaded = match CHUNKS_WRITTEN.get(&file_header_hash) {
        Some(c) => {
            c.1.values()
                .map(|uploaded| match uploaded {
                    true => 0,
                    false => 1,
                })
                .sum()
        }
        None => file_header.chunks.len(),
    };

    for _ in 0..chunks_being_uploaded {
        let chunk_metadata_len = sock.read_u16().await? as usize;
        sock.read_exact(&mut chunk_metadata_buf[..chunk_metadata_len])
            .await?;

        trace!("Chunk metadata is {chunk_metadata_len} bytes");
        let chunk_metadata =
            rkyv::check_archived_root::<ChunkMetadata>(&chunk_metadata_buf[..chunk_metadata_len])
                .map_err(|_| anyhow!("Error deserializing chunk metadata"))?;

        sock.read_exact(&mut chunk_buf[..chunk_metadata.size as usize])
            .await?;
        let chunk_buf = &chunk_buf[..chunk_metadata.size as usize];

        let mut hasher = Hasher::new();
        match use_parallel_hasher {
            true => hasher.update_rayon(chunk_buf),
            false => hasher.update(chunk_buf),
        };

        // Check the hash of the chunk
        if hasher.finalize().to_string() != chunk_metadata.hash {
            todo!("Sent bad chunk")
        }

        // Copy the chunk into the file
        let chunk_byte_index = chunk_metadata.id * file_header.chunk_size as u64;

        tmp_file
            .seek(std::io::SeekFrom::Start(chunk_byte_index))
            .await?;
        tmp_file.write_all(chunk_buf).await?;
        tmp_file.rewind().await?;

        let file_header: FileHeader = file_header.deserialize(&mut rkyv::Infallible)?;
        let chunks_written = &mut match CHUNKS_WRITTEN.get_mut(&file_header_hash) {
            Some(c) => c,
            None => {
                CHUNKS_WRITTEN.insert(
                    file_header_hash,
                    (
                        file_header.clone(),
                        file_header
                            .chunks
                            .keys()
                            .copied()
                            .map(|chunk_id| (chunk_id, false))
                            .collect(),
                    ),
                );
                CHUNKS_WRITTEN.get_mut(&file_header_hash).unwrap()
            }
        };

        // The chunk has been successfully written!
        *chunks_written.1.get_mut(&chunk_metadata.id).unwrap() = true;

        // If every chunk has been written, the file is completed
        if chunks_written.1.values().copied().all(|written| written) {
            break;
        }
    }

    //When th efile has finished being uplaoded, move it to the proper directory
    fs::rename(
        tmp_file_path,
        format!("./files/{}", &hex_string(&file_header.hash)),
    )
    .await?;

    debug!("Finished receiving file: {}", hex_string(&file_header.hash));

    Ok(())
}
