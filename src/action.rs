use anyhow::anyhow;
use bfsp::internal::ActionInfo;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::error_span;
use tracing::{error, Level};

use rand::Rng;

use crate::{chunk_db::ChunkDB, meta_db::MetaDB};

#[derive(Debug)]
enum Action {
    DeleteFiles,
    SuspendRead,
    SuspendWrite,
    SuspendDelete,
    SuspendQuery,
}

impl TryFrom<String> for Action {
    type Error = anyhow::Error;

    #[tracing::instrument(err)]
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "delete_files" => Ok(Self::DeleteFiles),
            "suspend_read" => Ok(Self::SuspendRead),
            "suspend_write" => Ok(Self::SuspendWrite),
            "suspend_delete" => Ok(Self::SuspendDelete),
            "suspend_query" => Ok(Self::SuspendQuery),
            _ => Err(anyhow!("invalid action")),
        }
    }
}

pub async fn check_run_actions_loop<M: MetaDB + 'static, C: ChunkDB + 'static>(
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
) {
    loop {
        tracing::span!(Level::INFO, "run_current_actions");

        match meta_db
            .list_actions(Some("pending".to_string()), true)
            .await
        {
            Ok(actions) => {
                for action_info in actions.into_iter() {
                    let meta_db = Arc::clone(&meta_db);
                    let chunk_db = Arc::clone(&chunk_db);

                    tokio::task::spawn(async move {
                        match run_action(Arc::clone(&meta_db), chunk_db, &action_info).await {
                            Ok(_) => {
                                let _ = meta_db.executed_action(action_info.id.unwrap()).await;
                            }
                            Err(err) => {
                                error!("Error running action: {err}");
                            }
                        }
                    });
                }
            }
            Err(err) => {
                error!("Error listing actions: {err}");
            }
        }

        // random jitter to make servers less likely to run multiple actions at once
        let jitter = rand::thread_rng().gen_range(-1.0..=1.0);
        tokio::time::sleep(Duration::from_secs_f32(300.0 + jitter)).await;
    }
}

#[tracing::instrument(err, skip(meta_db, chunk_db))]
async fn run_action<M: MetaDB, C: ChunkDB>(
    meta_db: Arc<M>,
    chunk_db: Arc<C>,
    action_info: &ActionInfo,
) -> anyhow::Result<()> {
    let action: Action = action_info.action.clone().try_into()?;
    let user_id = action_info.user_id;

    match action {
        Action::DeleteFiles => {
            meta_db.delete_all_meta(user_id).await?;
        }
        //FIXME: REPLACE ALL WITH JSON_INSERT FUNCTION!!!!!!
        Action::SuspendRead => {
            // FIXME: i'm so god damn sure i can just do something like suspension_info->read = true;
            let suspensions = meta_db.suspensions(&[user_id]).await?;
            let mut suspension = suspensions[&user_id];
            suspension.read_suspended = true;
            meta_db
                .set_suspensions([(user_id, suspension)].into())
                .await?;
        }
        Action::SuspendWrite => {
            // FIXME: i'm so god damn sure i can just do something like suspension_info->read = true;
            let suspensions = meta_db.suspensions(&[user_id]).await?;
            let mut suspension = suspensions[&user_id];
            suspension.write_suspended = true;
            meta_db
                .set_suspensions([(user_id, suspension)].into())
                .await?;
        }
        Action::SuspendDelete => {
            // FIXME: i'm so god damn sure i can just do something like suspension_info->read = true;
            let suspensions = meta_db.suspensions(&[user_id]).await?;
            let mut suspension = suspensions[&user_id];
            suspension.delete_suspended = true;
            meta_db
                .set_suspensions([(user_id, suspension)].into())
                .await?;
        }
        Action::SuspendQuery => {
            // FIXME: i'm so god damn sure i can just do something like suspension_info->read = true;
            let suspensions = meta_db.suspensions(&[user_id]).await?;
            let mut suspension = suspensions[&user_id];
            suspension.query_suspended = true;
            meta_db
                .set_suspensions([(user_id, suspension)].into())
                .await?;
        }
    };

    Ok(())
}
