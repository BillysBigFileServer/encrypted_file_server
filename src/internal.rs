use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bfsp::internal::get_queued_action_resp::{Actions, ActionsPerUser};
use bfsp::internal::internal_file_server_message::Message;
use bfsp::internal::{
    get_queued_action_resp, queue_action_resp, DeleteQueuedActionResp, GetQueuedActionResp,
    GetStorageCapResp, GetSuspensionsResp, QueueActionResp, SuspendUsersResp,
};
use bfsp::{
    chacha20poly1305::XChaCha20Poly1305,
    internal::{
        decrypt_internal_message, EncryptedInternalFileServerMessage, GetUsageResp,
        SetStorageCapResp,
    },
};
/// The internal API
use bfsp::{Message as ProtoMessage, PrependLen};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{event, Level};

use crate::meta_db::MetaDB;

#[tracing::instrument(skip(key, msg, meta_db))]
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
        Message::GetStorageCap(query) => {
            let user_ids = query.user_ids;
            let caps = meta_db.storage_caps(&user_ids).await.unwrap();

            GetStorageCapResp {
                response: Some(bfsp::internal::get_storage_cap_resp::Response::StorageCaps(
                    bfsp::internal::get_storage_cap_resp::StorageCap { storage_caps: caps },
                )),
            }
            .encode_to_vec()
        }
        Message::SetStorageCap(query) => {
            let caps = query.storage_caps;
            meta_db.set_storage_caps(caps).await.unwrap();

            SetStorageCapResp { err: None }.encode_to_vec()
        }
        Message::GetSuspensions(args) => {
            let user_ids = args.user_ids;
            let suspensions = meta_db.suspensions(&user_ids).await.unwrap();

            GetSuspensionsResp {
                response: Some(bfsp::internal::get_suspensions_resp::Response::Suspensions(
                    bfsp::internal::get_suspensions_resp::Suspensions {
                        suspension_info: suspensions,
                    },
                )),
            }
            .encode_to_vec()
        }
        Message::SuspendUsers(args) => {
            let suspensions = args.suspensions;
            meta_db.set_suspensions(suspensions).await.unwrap();

            SuspendUsersResp { err: None }.encode_to_vec()
        }
        Message::QueueAction(args) => {
            let action_info = args.action.unwrap();
            let action = meta_db.queue_action(action_info).await.unwrap();

            QueueActionResp {
                response: Some(queue_action_resp::Response::Action(action)),
            }
            .encode_to_vec()
        }
        Message::GetQueuedActions(args) => {
            let user_ids: HashSet<i64> = args.user_ids.into_iter().collect();
            let actions: HashMap<i64, Actions> = meta_db
                .get_actions_for_users(user_ids)
                .await
                .unwrap()
                .into_iter()
                .map(|(user_id, actions)| (user_id, Actions { actions }))
                .collect();

            GetQueuedActionResp {
                response: Some(get_queued_action_resp::Response::Actions(ActionsPerUser {
                    action_info: actions,
                })),
            }
            .encode_to_vec()
        }
        Message::DeleteQueuedAction(args) => {
            let action_id = args.action_id;
            meta_db.delete_action(action_id).await.unwrap();

            DeleteQueuedActionResp { err: None }.encode_to_vec()
        }
    }
    .prepend_len()
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
    loop {
        let internal_private_key = internal_private_key.clone();
        event!(Level::INFO, "Waiting for message");

        let len = match read_sock.read_u32_le().await {
            Ok(len) => len,
            Err(err) => {
                event!(Level::ERROR, error = err.to_string(), "Connection closed");
                return;
            }
        };
        event!(Level::INFO, "Message length: {}", len);

        if len > 8 * (1024_u32).pow(2) {
            event!(Level::WARN, "Unsually large message");
        }

        let mut buf = vec![0; len as usize];
        read_sock.read_exact(&mut buf).await.unwrap();
        event!(Level::INFO, "Message received");

        event!(Level::INFO, "Decoding encrypted message");
        let enc_message = EncryptedInternalFileServerMessage::decode(buf.as_slice()).unwrap();
        event!(Level::INFO, "Decoded encrypted message");
        let resp =
            handle_internal_message(meta_db.as_ref(), internal_private_key, enc_message).await;

        event!(Level::INFO, "Sending response");
        write_sock.write_all(resp.as_slice()).await.unwrap();
        event!(Level::INFO, "Response sent");
    }
}
