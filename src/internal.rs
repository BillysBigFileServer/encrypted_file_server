use std::sync::Arc;

use bfsp::internal::internal_file_server_message::Message;
/// The internal API
use bfsp::Message as ProtoMessage;
use bfsp::{
    chacha20poly1305::XChaCha20Poly1305,
    internal::{decrypt_internal_message, EncryptedInternalFileServerMessage, GetUsageResp},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{event, Level};
use wtransport::endpoint::IncomingSession;

use crate::meta_db::MetaDB;

#[tracing::instrument(skip(key))]
async fn handle_internal_message<M: MetaDB>(
    meta_db: &M,
    key: XChaCha20Poly1305,
    msg: EncryptedInternalFileServerMessage,
) -> Vec<u8> {
    let msg = decrypt_internal_message(key, msg);
    match msg.message.unwrap() {
        Message::GetUsage(query) => {
            let user_ids = query.user_ids;
            let usages = meta_db.total_usages(&user_ids).await.unwrap();

            GetUsageResp {
                response: Some(bfsp::internal::get_usage_resp::Response::Usage(
                    bfsp::internal::get_usage_resp::Usage { usages },
                )),
            }
            .encode_to_vec()
        }
    }
}

#[tracing::instrument(skip(stream, internal_private_key))]
pub async fn handle_internal_connection<M: MetaDB + 'static>(
    stream: TcpStream,
    internal_private_key: XChaCha20Poly1305,
    meta_db: Arc<M>,
) {
    let (mut read_sock, mut write_sock) = stream.into_split();
    // A single socket can have multiple connections. Multiplexing!
    let meta_db = Arc::clone(&meta_db);

    let internal_private_key = internal_private_key.clone();
    tokio::task::spawn(async move {
        event!(Level::INFO, "Waiting for message");

        let len = read_sock.read_u32().await.unwrap();
        event!(Level::INFO, "Message length: {}", len);

        let mut buf = vec![0; len as usize];
        read_sock.read_exact(&mut buf).await.unwrap();
        event!(Level::INFO, "Message received");

        event!(Level::INFO, "Decoding encrypted message");
        let enc_message = EncryptedInternalFileServerMessage::decode(buf.as_slice()).unwrap();
        event!(Level::INFO, "Decoded encrypted message");
        let resp =
            handle_internal_message(meta_db.as_ref(), internal_private_key, enc_message).await;

        write_sock.write_all(resp.as_slice()).await.unwrap();
    });
}
