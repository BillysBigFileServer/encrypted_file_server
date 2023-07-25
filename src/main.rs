mod auth;

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bfsp::*;
use blake3::Hasher;
use log::{info, trace};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::{
    fs::File,
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

    info!("Starting server!");

    let sock = TcpListener::bind(":::9999").await.unwrap();
    while let Ok((mut sock, _addr)) = sock.accept().await {
        tokio::task::spawn(async move {
            let action: Action = sock.read_u16().await.unwrap().try_into().unwrap();

            match action {
                Action::Upload => handle_upload(&mut sock).await.unwrap(),
            };
            sock.write_u8(0).await.unwrap();
        });
    }

    Ok(())
}

async fn handle_upload(sock: &mut TcpStream) -> Result<()> {
    // sock.read_exact(&mut buf[..blake3::OUT_LEN]).await?;
    // // FIXME: validate cookie
    // let cookie = String::from_utf8(buf[..blake3::OUT_LEN].to_vec())?;

    let mut file_header_buf = [0_u8; 1024];

    let file_header = {
        let file_header_len = sock.read_u16().await? as usize;
        sock.read_exact(&mut file_header_buf[..file_header_len])
            .await?;
        let file_header_bytes = &file_header_buf[..file_header_len];

        rkyv::check_archived_root::<FileHeader>(file_header_bytes)
            .map_err(|_| anyhow!("Error deserializing file header"))?
    };

    // Make sure the hash is an actual hash
    // This is a close enough approximation
    if !file_header.hash.chars().all(|c| c.is_alphanumeric())
        && file_header.hash.len() == blake3::OUT_LEN
    {
        return Err(anyhow!("Invalid file hash"));
    }

    // First, create the temporary file
    let mut file = tokio::fs::File::create(format!("/tmp/{}", &file_header.hash)).await?;
    // Next, fill it with zeroes
    file.set_len(file_header.total_file_size()).await?;

    // FIXME: Have a maximum chunk size
    let mut chunk_buf = vec![0; file_header.chunk_size as usize];
    let mut chunk_metadata_buf = [0; 1024];

    // TODO: Keep this in sqlite DB
    let mut chunks_written: HashMap<ChunkID, bool> = file_header
        .chunks
        .keys()
        .copied()
        .map(|chunk_id| (chunk_id, false))
        .collect();

    // Note: We don't check the final file hash of the file since, if all the chunk hashes match, so will the final hash
    let use_parallel_hasher = use_parallel_hasher(file_header.chunk_size as usize);

    loop {
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

        file.seek(std::io::SeekFrom::Start(chunk_byte_index))
            .await?;
        file.write_all(chunk_buf).await?;
        file.rewind().await?;

        // The chunk has been successfully written!
        *chunks_written.get_mut(&chunk_metadata.id).unwrap() = true;

        // If every chunk has been written, the file is completed
        if chunks_written.values().copied().all(|written| written) {
            break;
        }
    }

    trace!("Finished receiving file: {}", file_header.hash);

    Ok(())
}
