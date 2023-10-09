mod crypto;
mod db;

use anyhow::{anyhow, Context, Error};
use crypto::init_key;
use sqlx::{FromRow, Row, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt::Display;
use std::fs::canonicalize;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use bfsp::{
    chunk_from_file, hash_file, use_parallel_hasher, Action, ChunkHash, ChunkID, ChunkMetadata,
    ChunksUploaded, ChunksUploadedQuery, DownloadChunkReq, DownloadReq, FileHash, FileHeader, hash_chunk, parallel_hash_chunk,
};
use log::{debug, info, trace};
use rkyv::{Deserialize, DeserializeUnsized};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
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
        .chain(fern::log_file("output.log").unwrap())
        // Apply globally
        .apply()
        .unwrap();

    let pool = sqlx::SqlitePool::connect(&env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    //init_key().await.unwrap();

    let mut sock = TcpStream::connect("127.0.0.1:9999").await.unwrap();

    if let Err(err) = &add_file("../bfsp/test_files/tux_huge.png", &pool).await {
        if let Some(err) = err.downcast_ref::<AddFileRecoverableErr>() {
            match err {
                AddFileRecoverableErr::FileAlreadyAdded => (),
                AddFileRecoverableErr::ChunkAlreadyAdded => (),
            }
        } else {
            panic!("Error adding file: {err}");
        }
    }

    sync_files(&pool, &mut sock).await.unwrap();
}

pub async fn sync_files(pool: &sqlx::SqlitePool, sock: &mut TcpStream) -> Result<()> {
    trace!("Starting file sync");

    loop {
        // TODO: maybe use an iterator instead
        let files = sqlx::query!("select hash, chunk_size, file_path from files")
            .fetch_all(pool)
            .await?;

        // TODO: parallelize
        for file_info in files.iter() {
            let file_path = &file_info.file_path;
            trace!("Syncing file {file_path}");

            let mut file = match OpenOptions::new().read(true).open(file_path).await {
                Ok(file) => file,
                Err(_) => todo!("Download file"),
            };

            if let Ok(is_valid) = validate_file(file_path, pool).await {
                if !is_valid {
                    todo!()
                }
            }

            // Check that the hashes are the same
            let true_hash = hash_file(&mut file).await?;

            let file_info_hash: FileHash = file_info.hash.clone().try_into()?;

            // The file has change, re add it
            if true_hash != file_info_hash {
                update_file(file_path, &mut file, pool).await?;
                continue;
            }

            let chunks = sqlx::query!(
                "select hash, id, indice, chunk_size as size from chunks where file_hash = ?",
                file_info_hash
            )
            .fetch_all(pool)
            .await
            .with_context(|| "Error querying db for chunks: ")?;

            let chunk_indices = sqlx::query!(
                "select indice, id from chunks where file_hash = ?",
                file_info.hash
            )
            .fetch_all(pool)
            .await
            .with_context(|| "Error querying db for chunk indicecs: ")?
            .into_iter()
            .map(|chunk_info| {
                let chunk_id: ChunkID = chunk_info.id.try_into().unwrap();
                let chunk_indice: u64 = chunk_info.indice.try_into().unwrap();
                (chunk_id, chunk_indice)
            })
            .collect();

            debug_assert!(chunks.len() != 0);

            let file_header = FileHeader {
                hash: file_info_hash,
                chunk_size: file_info.chunk_size.try_into().unwrap(),
                chunks: chunks
                    .into_iter()
                    .map(|chunk_meta| {
                        let chunk_id: ChunkID = chunk_meta.id.try_into().unwrap();
                        (
                            chunk_id,
                            ChunkMetadata {
                                id: chunk_id,
                                hash: chunk_meta.hash.try_into().unwrap(),
                                size: chunk_meta.size as u32,
                                indice: chunk_meta.indice.try_into().unwrap(),
                            },
                        )
                    })
                    .collect(),
                chunk_indices,
            };

            upload_file(&mut file, &file_header, sock, pool)
                .await
                .with_context(|| "Error uploading file for sync: ")?;

            debug!("Finished syncing file {file_path}");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn query_chunks_uploaded(
    file_hash: &FileHash,
    sock: &mut TcpStream,
    pool: &SqlitePool,
) -> Result<ChunksUploaded> {
    trace!("Querying chunks uploaded");
    let action: u16 = Action::QueryChunksUploaded.into();

    let chunk_ids = sqlx::query!("select id from chunks where file_hash = ?", file_hash)
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to get chunk IDs for file hash {}", file_hash))?
        .into_iter()
        .map(|chunk_info| chunk_info.id.try_into())
        .collect::<Result<HashSet<ChunkID>>>()?;

    let chunks_uploaded_query = ChunksUploadedQuery { chunks: chunk_ids };

    sock.write_u16(action).await?;
    sock.write_all(chunks_uploaded_query.to_bytes()?.as_slice())
        .await?;

    let parts_uploaded_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; parts_uploaded_len];

    sock.read_exact(&mut buf).await.unwrap();

    let chunks_uploaded = ChunksUploaded::try_from_bytes(&buf)?;
    let res = chunks_uploaded.deserialize(&mut rkyv::Infallible)?;

    trace!("Finished querying chunks uploaded");

    Ok(res)
}

#[derive(Debug)]
pub enum AddFileRecoverableErr {
    FileAlreadyAdded,
    ChunkAlreadyAdded,
}

impl Display for AddFileRecoverableErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AddFileRecoverableErr::FileAlreadyAdded => "File already added",
            AddFileRecoverableErr::ChunkAlreadyAdded => "Chunk already added",
        })
    }
}

impl std::error::Error for AddFileRecoverableErr {}

// TODO: delete unused chunks on server
pub async fn update_file(file_path: &str, file: &mut File, pool: &SqlitePool) -> Result<()> {
    trace!("Updating file {file_path}");

    let file_path = fs::canonicalize(file_path).await?;
    let file_path = file_path.to_str().unwrap();

    let original_file_hash: FileHash =
        sqlx::query!("select hash from files where file_path = ?", file_path)
            .fetch_one(pool)
            .await
            .with_context(|| "Error getting file id for updating file")?
            .hash
            .try_into()?;

    let file_header = FileHeader::from_file(file).await?;
    file.rewind().await?;
    let file_hash = hash_file(file).await?;
    file.rewind().await?;

    let num_chunks: i64 = file_header.chunks.len().try_into().unwrap();

    sqlx::query!(
        "update files set hash = ?, chunk_size = ?, chunks = ? where file_path = ?",
        file_hash,
        file_header.chunk_size,
        num_chunks,
        file_path
    )
    .execute(pool)
    .await
    .map_err(|err| match err {
        sqlx::Error::Database(err) => match err.kind() {
            sqlx::error::ErrorKind::UniqueViolation => {
                Error::new(AddFileRecoverableErr::FileAlreadyAdded)
            }
            _ => anyhow!("Unknown database error: {err:#}"),
        },
        _ => anyhow!("Unknown database error: {err:#}"),
    })?;

    trace!("Deleting chunks for file {file_path}");

    sqlx::query!("delete from chunks where file_hash = ?", original_file_hash)
        .execute(pool)
        .await?;

    trace!("Inserting chunks for file {file_path}");

    for (chunk_id, chunk) in file_header.chunks.iter() {
        sqlx::query!(
            r#"insert into chunks
                (hash, id, file_hash, chunk_size)
                values ( ?, ?, ?, ? )
            "#,
            chunk.hash,
            chunk_id,
            file_header.hash,
            chunk.size,
        )
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn add_file(file_path: &str, pool: &SqlitePool) -> Result<()> {
    trace!("Adding file {file_path}");

    let file_path = fs::canonicalize(file_path).await?;
    let file_path = file_path.to_str().unwrap();

    let mut file = fs::File::open(file_path)
        .await
        .with_context(|| format!("Failed to read file {file_path} for adding"))?;
    let file_header = FileHeader::from_file(&mut file).await?;
    file.rewind().await?;
    let file_hash = hash_file(&mut file).await?;
    file.rewind().await?;

    let num_chunks: i64 = file_header.chunks.len().try_into().unwrap();

    sqlx::query!(
        r#"insert into files
            (file_path, hash, chunk_size, chunks)
            values ( ?, ?, ?, ? )
    "#,
        file_path,
        file_hash,
        file_header.chunk_size,
        num_chunks,
    )
    .execute(pool)
    .await
    .map_err(|err| match err {
        sqlx::Error::Database(err) => match err.kind() {
            sqlx::error::ErrorKind::UniqueViolation => {
                Error::new(AddFileRecoverableErr::FileAlreadyAdded)
            }
            _ => anyhow!("Unknown database error: {err:#}"),
        },
        _ => anyhow!("Unknown database error: {err:#}"),
    })?;

    trace!("Inserting chunks");

    //TODO: batch job this
    for (chunk_id, chunk) in file_header.chunks.iter() {
        let indice: i64 = (*file_header.chunk_indices.get(&chunk_id).unwrap())
            .try_into()
            .unwrap();

        sqlx::query!(
            r#"insert into chunks
                (hash, id, chunk_size, indice, file_hash)
                values ( ?, ?, ?, ?, ? )
            "#,
            chunk.hash,
            chunk_id,
            chunk.size,
            indice,
            file_hash,
        )
        .execute(pool)
        .await
        .map_err(|err| match err {
            sqlx::Error::Database(err) => match err.kind() {
                sqlx::error::ErrorKind::UniqueViolation => {
                    Error::new(AddFileRecoverableErr::ChunkAlreadyAdded)
                }
                _ => anyhow!("Unknown database error: {err:#}"),
            },
            _ => anyhow!("Unknown database error: {err:#}"),
        })?;
    }

    Ok(())
}

pub async fn upload_file(
    file: &mut File,
    file_header: &FileHeader,
    sock: &mut TcpStream,
    pool: &SqlitePool,
) -> Result<()> {
    trace!("Uploading file");

    let chunks_to_upload = sqlx::query!(
        "select id, hash, indice, chunk_size as size  from chunks where uploaded = false AND file_hash = ?",
        file_header.hash
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|chunk_info| {
        ChunkMetadata {
            id: chunk_info.id.try_into().unwrap(),
            hash: chunk_info.hash.try_into().unwrap(),
            size: chunk_info.size.try_into().unwrap(),
            indice: chunk_info.indice.try_into().unwrap(),
        }
    });

    trace!("Uploading {} chunks", chunks_to_upload.len());

    for chunk_meta in chunks_to_upload {
        trace!("Uploading chunk {}", chunk_meta.id);

        let action = Action::UploadChunk.into();

        sock.write_u16(action).await?;
        sock.write_all(chunk_meta.to_bytes()?.as_slice()).await?;

        let chunk = chunk_from_file(file_header, file, chunk_meta.id).await?;
        sock.write_all(chunk.as_slice()).await?;
    }

    trace!("Uploaded file");

    Ok(())
}

pub async fn download_file<P: AsRef<Path> + Display>(
    file_header: &FileHeader,
    file_path: P,
    sock: &mut TcpStream,
    pool: &SqlitePool,
) -> Result<()> {
    trace!("Downloading file {file_path}");

    let chunks_uploaded = query_chunks_uploaded(&file_header.hash, sock, pool).await?;
    let chunks_uploaded = &chunks_uploaded.chunks;
    let chunks_to_download = file_header
        .chunks
        .keys()
        .filter(|chunk_id| !chunks_uploaded.get(chunk_id).unwrap());

    let action: u16 = Action::DownloadChunk.into();

    trace!("Creating or opening file {file_path}");

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&file_path)
        .await?;

    trace!("Opened file {file_path}");

    let use_parallel_hasher = use_parallel_hasher(file_header.chunk_size as usize);
    let hash_chunk_fn = match use_parallel_hasher {
        true => parallel_hash_chunk,
        false => hash_chunk,
    };

    for chunk_id in chunks_to_download {
        sock.write_u16(action).await?;
        let download_chunk_req = DownloadChunkReq {
            chunk_id: *chunk_id,
        };
        sock.write_all(download_chunk_req.to_bytes()?.as_slice())
            .await?;

        let chunk_metadata_len = sock.read_u16().await? as usize;
        let mut chunk_metadata_buf = vec![0; chunk_metadata_len];

        sock.read_exact(&mut chunk_metadata_buf).await?;

        trace!("Chunk metadata is {chunk_metadata_len} bytes");
        let chunk_metadata = rkyv::check_archived_root::<ChunkMetadata>(&chunk_metadata_buf)
            .map_err(|_| anyhow!("Error deserializing chunk metadata"))?;

        let mut chunk_buf = vec![0; chunk_metadata.size as usize];

        sock.read_exact(&mut chunk_buf).await?;

        let chunk_hash = hash_chunk_fn(&chunk_buf);

        // Check the hash of the chunk
        if chunk_hash != chunk_metadata.hash {
            todo!("Sent bad chunk")
        }

        // Copy the chunk into the file
        let chunk_byte_index = chunk_metadata.indice * file_header.chunk_size as u64;

        file.seek(std::io::SeekFrom::Start(chunk_byte_index))
            .await?;
        file.write_all(&chunk_buf).await?;
        file.rewind().await?;
    }

    debug!("Finished downloading file {file_path}");

    Ok(())
}

/// Ensures that the file given and file header as exists in the database are the same
async fn validate_file(file_path: &str, pool: &SqlitePool) -> Result<bool> {
    let file_info = sqlx::query!(
        "select file_path, hash, chunk_size from files where file_path = ?",
        file_path
    )
    .fetch_one(pool)
    .await?;

    let chunk_size = file_info.chunk_size;
    let file_hash: FileHash = file_info.hash.try_into()?;

    // TODO: return cstuom error for when file isn't added
    let chunks = sqlx::query!(
        "select hash, id, chunk_size, indice from chunks where file_hash = ?",
        file_hash
    )
    .fetch_all(pool)
    .await
    .with_context(|| "Error querying db for chunks: ")?;

    let chunks = chunks.into_iter().map(|chunk| ChunkMetadata {
        id: chunk.id.try_into().unwrap(),
        hash: chunk.hash.try_into().unwrap(),
        size: chunk.chunk_size.try_into().unwrap(),
        indice: chunk.indice.try_into().unwrap(),
    });

    let chunk_indices: HashMap<ChunkID, u64> = sqlx::query!(
        "select indice, id from chunks where file_hash = ?",
        file_hash
    )
    .fetch_all(pool)
    .await
    .with_context(|| "Error querying db for chunk indicecs: ")?
    .into_iter()
    .map(|chunk_info| {
        let chunk_id: ChunkID = chunk_info.id.try_into().unwrap();
        let chunk_indice: u64 = chunk_info.indice.try_into().unwrap();

        (chunk_id, chunk_indice)
    })
    .collect();

    let chunks = chunks.into_iter().map(|chunk| (chunk.id, chunk)).collect();

    let file_header_sql = FileHeader {
        hash: file_hash,
        chunk_size: chunk_size.try_into().unwrap(),
        chunks,
        chunk_indices,
    };

    let mut file = File::open(file_path).await?;
    let file_header = FileHeader::from_file(&mut file).await?;

    debug_assert_eq!(file_header_sql.chunk_indices, file_header.chunk_indices);
    debug_assert_eq!(file_header_sql.chunks, file_header.chunks);

    return Ok(file_header_sql == file_header);
}
