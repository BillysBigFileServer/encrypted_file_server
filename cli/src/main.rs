use anyhow::anyhow;
use std::fmt::Display;
use std::path::Path;

use anyhow::Result;
use bfsp::{
    chunk_from_file, use_parallel_hasher, Action, ArchivedChunksUploaded, ChunkID, ChunkMetadata,
    ChunksUploaded, DownloadReq, FileHeader,
};
use blake3::{Hash, Hasher};
use log::{debug, info, trace};
use rkyv::Deserialize;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
// use iced::widget::{button, column, text};

// struct App {}
// #[derive(Clone, Debug)]
// enum Message {}

// use iced::{Alignment, Application};
// struct Settings {}

// impl Application for App {
//     type Executor = iced::executor::Default;
//     type Message = Message;
//     type Theme = iced::Theme;
//     type Flags = Settings;

//     fn new(flags: Self::Flags) -> (Self, iced::Command<Self::Message>) {
//         (Self {}, iced::Command::none())
//     }

//     fn title(&self) -> String {
//         "hello".to_string()
//     }

//     fn update(&mut self, message: Self::Message) -> iced::Command<Self::Message> {
//         iced::Command::none()
//     }

//     fn view(&self) -> iced::Element<Message> {
//         column![
//             button("Increment"),
//             text("hi").size(50),
//             button("Decrement")
//         ]
//         .padding(20)
//         .align_items(Alignment::Center)
//         .into()
//     }
// }

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
        // - and per-module overrides
        // Output to stdout, files, and other Dispatch configurations
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log").unwrap())
        // Apply globally
        .apply()
        .unwrap();

    let mut sock = TcpStream::connect("127.0.0.1:9999").await.unwrap();
    let file_name = "../bfsp/test_files/tux_huge.png";

    let mut file = File::open(&file_name).await.unwrap();
    let file_header = FileHeader::from_file(&mut file).await.unwrap();

    upload_file(
        &ChunksUploaded { chunks: None },
        &mut file,
        &file_header,
        &mut sock,
    )
    .await
    .unwrap();

    download_file(&file_header, "./tux.png", &mut sock)
        .await
        .unwrap();

    // App::run(iced::Settings::with_flags(Settings {})).unwrap()
}

pub async fn query_chunks_uploaded(
    file_header: &FileHeader,
    sock: &mut TcpStream,
) -> Result<ChunksUploaded> {
    trace!("Querying chunks uploaded");
    let action: u16 = Action::QueryPartialUpload.into();

    sock.write_u16(action).await.unwrap();
    sock.write_all(&file_header.hash).await?;

    let mut buf = [0; 1024];

    let parts_uploaded_len = sock.read_u16().await? as usize;
    sock.read_exact(&mut buf[..parts_uploaded_len])
        .await
        .unwrap();

    let chunks_uploaded = ChunksUploaded::try_from_bytes(&buf[..parts_uploaded_len])?;
    let res = chunks_uploaded.deserialize(&mut rkyv::Infallible)?;
    trace!("Finished querying chunks uploaded");

    Ok(res)
}

pub async fn upload_file(
    parts_uploaded: &ChunksUploaded,
    file: &mut File,
    file_header: &FileHeader,
    sock: &mut TcpStream,
) -> Result<()> {
    trace!("Uploading file");

    let chunks_to_upload: Vec<ChunkID> = match &parts_uploaded.chunks {
        Option::Some(chunks) => chunks
            .iter()
            .filter_map(|(chunk_id, uploaded)| match *uploaded {
                false => Some(chunk_id),
                true => None,
            })
            .copied()
            .collect(),
        Option::None => (0..file_header.chunks.len() as ChunkID).collect(),
    };

    let action = Action::Upload.into();
    sock.write_u16(action).await?;
    sock.write_all(&file_header.to_bytes()?).await?;

    for chunk_id in chunks_to_upload {
        trace!("Uploading chunk {chunk_id}");

        let chunk_meta = file_header.chunks.get(&chunk_id).unwrap();
        sock.write_all(&chunk_meta.to_bytes()?).await?;
        let chunk = chunk_from_file(file_header, file, chunk_id).await?;
        sock.write_all(&chunk).await?;
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

    let chunks_uploaded = query_chunks_uploaded(file_header, sock).await?;

    let action: u16 = Action::Download.into();
    sock.write_u16(action).await?;

    trace!("Sending download request");

    let download_req = DownloadReq {
        file_hash: file_header.hash,
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
        let chunk_byte_index = chunk_metadata.id * file_header.chunk_size as u64;

        file.seek(std::io::SeekFrom::Start(chunk_byte_index))
            .await?;
        file.write_all(chunk_buf).await?;
        file.rewind().await?;
    }

    debug!("Finished downloading file {file_path}");

    Ok(())
}
