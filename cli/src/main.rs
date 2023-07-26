use std::str::FromStr;

use bfsp::{chunk_from_file, Action, ChunkID, ChunksUploaded, FileHeader};
use blake3::Hash;
use rkyv::option::ArchivedOption;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// use iced::widget::{button, column, text};
// use iced::{Alignment, Application};
// struct App {}
// #[derive(Clone, Debug)]
// enum Message {}

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
    let mut sock = TcpStream::connect("127.0.0.1:9999").await.unwrap();
    let file_name = "../bfsp/test_files/tux_huge.png";

    println!("Uploading file: {file_name}");

    let mut file = File::open(&file_name).await.unwrap();
    let file_header = FileHeader::from_file(&mut file).await.unwrap();

    let mut action: u16 = Action::QueryPartialUpload.into();
    sock.write_u16(action).await.unwrap();
    sock.write_all(Hash::from_str(&file_header.hash).unwrap().as_bytes())
        .await
        .unwrap();

    let mut buf = [0; 1024];

    let parts_uploaded_len = sock.read_u16().await.unwrap() as usize;
    sock.read_exact(&mut buf[..parts_uploaded_len])
        .await
        .unwrap();

    let parts_uploaded = ChunksUploaded::try_from_bytes(&buf[..parts_uploaded_len]).unwrap();
    let chunks_to_upload: Vec<ChunkID> = match &parts_uploaded.chunks {
        ArchivedOption::Some(chunks) => chunks
            .iter()
            .filter_map(|(chunk_id, uploaded)| match *uploaded {
                false => Some(chunk_id),
                true => None,
            })
            .copied()
            .collect(),
        ArchivedOption::None => (0..file_header.chunks.len() as ChunkID).collect(),
    };

    println!("{:?}", parts_uploaded.chunks);

    action = Action::Upload.into();
    sock.write_u16(action).await.unwrap();
    sock.write_all(&file_header.to_bytes().unwrap())
        .await
        .unwrap();

    for chunk_id in chunks_to_upload {
        println!("Uploading chunk {chunk_id}");

        let chunk_meta = file_header.chunks.get(&chunk_id).unwrap();
        sock.write_all(&chunk_meta.to_bytes().unwrap())
            .await
            .unwrap();
        let chunk = chunk_from_file(&file_header, &mut file, chunk_id)
            .await
            .unwrap();
        sock.write_all(&chunk).await.unwrap();
    }

    // App::run(iced::Settings::with_flags(Settings {})).unwrap()
}
