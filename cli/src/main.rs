mod db;

use anyhow::{anyhow, Context, Error};
use sqlx::{FromRow, Row, SqlitePool};
use std::env;
use std::fmt::Display;
use std::fs::canonicalize;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use bfsp::{
    chunk_from_file, hash_file, use_parallel_hasher, Action, ChunkID, ChunkMetadata,
    ChunksUploaded, DownloadReq, FileHeader,
};
use blake3::Hasher;
use log::{debug, info, trace};
use rkyv::Deserialize;
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

    let mut sock = TcpStream::connect("127.0.0.1:9999").await.unwrap();

    if let Err(err) = &add_file("../bfsp/test_files/tux_huge.png", &pool).await {
        if let Some(err) = err.downcast_ref::<AddFileRecoverableErr>() {
            match err {
                AddFileRecoverableErr::FileAlreadyAdded => (),
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
        let files = sqlx::query!("select id, hash, chunk_size, file_path from files")
            .fetch_all(pool)
            .await?;

        // TODO: parallelize
        for file_info in files.iter() {
            let file_path = match file_info.file_path.as_ref() {
                Some(file_path) => file_path,
                None => todo!("Download file"),
            };
            trace!("Syncing file {file_path}");

            let mut file = match OpenOptions::new().read(true).open(file_path).await {
                Ok(file) => file,
                Err(_) => todo!("Download file"),
            };

            if let Ok(is_valid) = validate_file(file_path, pool).await {
                if !is_valid {
                    add_file(file_path, pool).await?;
                }
            }

            // Check that the hashes are the same
            let true_hash = hash_file(&mut file).await?;

            let file_info_hash =
                blake3::Hash::from_bytes(file_info.hash.clone().try_into().unwrap());

            // The file has change, re add it
            if true_hash != file_info_hash {
                update_file(file_path, &mut file, pool).await?;

                continue;
            }

            let chunks: Vec<ChunkMetadata> =
                sqlx::query_as("select hash, id, chunk_size from chunks where file_id = ?")
                    .bind(&file_info.id)
                    .fetch_all(pool)
                    .await
                    .with_context(|| "Error querying db for chunks: ")?;

            debug!("chunks: {:?}", chunks);
            debug_assert!(chunks.len() != 0);

            let file_header = FileHeader {
                id: file_info.id.clone().try_into().unwrap(),
                chunk_size: file_info.chunk_size.try_into().unwrap(),
                chunks: chunks
                    .into_iter()
                    .map(|chunk_meta| (chunk_meta.id, chunk_meta))
                    .collect(),
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
    file_id: &[u8; blake3::OUT_LEN],
    sock: &mut TcpStream,
) -> Result<ChunksUploaded> {
    trace!("Querying chunks uploaded");
    let action: u16 = Action::QueryPartialUpload.into();

    sock.write_u16(action).await.unwrap();
    sock.write_all(file_id).await?;

    let parts_uploaded_len = sock.read_u16().await? as usize;
    let mut buf = vec![0; parts_uploaded_len];

    sock.read_exact(&mut buf).await.unwrap();

    let chunks_uploaded = ChunksUploaded::try_from_bytes(&buf[..parts_uploaded_len])?;
    let res = chunks_uploaded.deserialize(&mut rkyv::Infallible)?;
    trace!("Finished querying chunks uploaded");

    Ok(res)
}

#[derive(Debug)]
pub enum AddFileRecoverableErr {
    FileAlreadyAdded,
}

impl Display for AddFileRecoverableErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AddFileRecoverableErr::FileAlreadyAdded => "File already added",
        })
    }
}

impl std::error::Error for AddFileRecoverableErr {}

pub async fn update_file(file_path: &str, file: &mut File, pool: &SqlitePool) -> Result<()> {
    trace!("Updating file {file_path}");

    let file_path = fs::canonicalize(file_path).await?;
    let file_path = file_path.to_str().unwrap();

    let file_header = FileHeader::from_file(file, file_path).await?;
    file.rewind().await?;
    let file_hash: blake3::Hash = hash_file(file).await?;
    file.rewind().await?;

    let num_chunks: i64 = file_header.chunks.len().try_into().unwrap();

    sqlx::query(
        "update files set id = ?, hash = ?, chunk_size = ?, chunks = ? where file_path = ?",
    )
    .bind(&file_header.id[..])
    .bind(&file_hash.as_bytes()[..])
    .bind(file_header.chunk_size)
    .bind(num_chunks)
    .bind(file_path)
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

    sqlx::query("delete from chunks where file_id = ?")
        .bind(&file_header.id[..])
        .execute(pool)
        .await?;

    trace!("Inserting chunks for file {file_path}");

    for (chunk_id, chunk) in file_header.chunks.iter() {
        sqlx::query(
            r#"insert into chunks
                (hash, id, file_id, chunk_size)
                values ( ?, ?, ?, ? )
            "#,
        )
        .bind(&chunk.hash[..])
        .bind(chunk_id)
        .bind(&file_header.id[..])
        .bind(chunk.size)
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
    let file_header = FileHeader::from_file(&mut file, file_path).await?;
    file.rewind().await?;
    let file_hash: blake3::Hash = hash_file(&mut file).await?;
    file.rewind().await?;

    let num_chunks: i64 = file_header.chunks.len().try_into().unwrap();

    sqlx::query(
        r#"insert into files
            (id, file_path, hash, chunk_size, chunks)
            values ( ?, ?, ?, ?, ? )
    "#,
    )
    .bind(&file_header.id[..])
    .bind(file_path)
    .bind(&file_hash.as_bytes()[..])
    .bind(file_header.chunk_size)
    .bind(num_chunks)
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
        sqlx::query(
            r#"insert into chunks
                (hash, id, file_id, chunk_size)
                values ( ?, ?, ?, ? )
            "#,
        )
        .bind(&chunk.hash[..])
        .bind(chunk_id)
        .bind(&file_header.id[..])
        .bind(chunk.size)
        .execute(pool)
        .await?;
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

    let chunks_to_upload: Vec<ChunkID> =
        sqlx::query("select id from chunks where uploaded = false AND file_id = ?")
            .bind(&file_header.id[..])
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();

    trace!("Uploading {} chunks", chunks_to_upload.len());

    let action = Action::Upload.into();
    sock.write_u16(action).await?;
    sock.write_all(&file_header.to_bytes()?).await?;

    for chunk_id in chunks_to_upload {
        trace!("Uploading chunk {chunk_id}");

        debug!("{:?}", file_header);
        let chunk_meta = file_header.chunks.get(&chunk_id).unwrap();
        sock.write_all(&chunk_meta.to_bytes()?).await?;
        let chunk = chunk_from_file(file_header, file, chunk_id).await?;
        sock.write_all(&chunk).await?;

        sqlx::query("update chunks set uploaded = ? where id = ?")
            .bind(true)
            .bind(chunk_id)
            .execute(pool)
            .await?;
    }

    trace!("Uploaded file");

    Ok(())
}

pub async fn download_file<P: AsRef<Path> + Display>(
    file_header: &FileHeader,
    file_path: P,
    sock: &mut TcpStream,
) -> Result<()> {
    trace!("Downloading file {file_path}");

    let chunks_uploaded = query_chunks_uploaded(&file_header.id, sock).await?;

    let action: u16 = Action::Download.into();
    sock.write_u16(action).await?;

    trace!("Sending download request");

    let download_req = DownloadReq {
        file_hash: file_header.id,
        chunks: chunks_uploaded
            .chunks
            .ok_or_else(|| anyhow!("File not found on server"))?
            .iter()
            .filter_map(|(chunk_id, done)| match done {
                true => Some(*chunk_id),
                false => None,
            })
            .collect(),
    };

    sock.write_all(&download_req.to_bytes()?).await?;

    trace!("Sent download request");

    let file_header_len = sock.read_u16().await? as usize;
    let mut file_header_buf = vec![0; file_header_len];
    sock.read_exact(&mut file_header_buf).await?;

    let file_header = rkyv::check_archived_root::<FileHeader>(&mut file_header_buf)
        .map_err(|_| anyhow!("Error deserializing file header"))?;
    let mut chunk_metadata_buf = vec![0; 1024];
    let mut chunk_buf = vec![0; file_header.chunk_size as usize];

    let use_parallel_hasher = use_parallel_hasher(file_header.chunk_size.try_into().unwrap());

    trace!("Creating or opening file {file_path}");

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&file_path)
        .await?;

    trace!("Created file {file_path}");

    for _ in 0..file_header.chunks.len() {
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
        let chunk_byte_index = (chunk_metadata.id * file_header.chunk_size) as u64;

        file.seek(std::io::SeekFrom::Start(chunk_byte_index))
            .await?;
        file.write_all(chunk_buf).await?;
        file.rewind().await?;
    }

    debug!("Finished downloading file {file_path}");

    Ok(())
}

/// Ensures that the file given and file header as exists in the database are the same
async fn validate_file(file_path: &str, pool: &SqlitePool) -> Result<bool> {
    let file_info = sqlx::query("select id, hash, chunk_size from files where file_path = ?")
        .bind(file_path)
        .fetch_one(pool)
        .await?;

    let file_id: &[u8] = file_info.get("id");
    let file_id: [u8; blake3::OUT_LEN] = file_id.try_into().unwrap();
    let chunk_size: u32 = file_info.get("chunk_size");

    // TODO: return cstuom error for when file isn't added
    let chunks: Vec<ChunkMetadata> =
        sqlx::query_as("select hash, id, chunk_size from chunks where file_id = ?")
            .bind(&file_id[..])
            .fetch_all(pool)
            .await
            .with_context(|| "Error querying db for chunks: ")?;

    let chunks = chunks.into_iter().map(|chunk| (chunk.id, chunk)).collect();

    let file_header_sql = FileHeader {
        id: file_id,
        chunk_size,
        chunks,
    };

    let mut file = File::open(file_path).await?;
    let file_header = FileHeader::from_file(&mut file, file_path).await?;

    return Ok(file_header_sql == file_header);
}
