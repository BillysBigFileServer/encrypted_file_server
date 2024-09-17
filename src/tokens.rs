use std::{collections::HashSet, env, sync::OnceLock, time::Duration};

use anyhow::anyhow;
use biscuit_auth::Biscuit;
use reqwest::StatusCode;
use tokio::sync::RwLock;

pub type RevocationIdentiifer = Vec<u8>;

static REVOKED_TOKENS: OnceLock<RwLock<HashSet<RevocationIdentiifer>>> = OnceLock::new();

#[tracing::instrument]
pub async fn refresh_revoked_tokens() {
    let mut token_num: u32 = 0;
    loop {
        match single_update_revoked_tokens(token_num, 100).await {
            Ok(tokens_inserted) => {
                // we don't want to get too far behind, we we should keep iterating up til we can't
                token_num += tokens_inserted;
                if tokens_inserted > 0 {
                    continue;
                }
            }
            Err(err) => {
                tracing::error!(
                    token_num = token_num,
                    page_size = 100,
                    "Error updating revoked tokens: {err}"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(5 * 60)).await;
    }
}

pub async fn check_token_revoked(token: &Biscuit) -> bool {
    for identifier in token.revocation_identifiers().iter() {
        let revoked_tokens = REVOKED_TOKENS.get_or_init(|| RwLock::new(HashSet::new()));
        if revoked_tokens.read().await.contains(identifier.as_slice()) {
            return true;
        }
    }

    false
}

#[tracing::instrument(err)]
async fn single_update_revoked_tokens(token_num: u32, page_size: u32) -> anyhow::Result<u32> {
    let revoked_tokens = REVOKED_TOKENS.get_or_init(|| RwLock::new(HashSet::new()));
    let big_central_url = big_central_url();
    let resp = reqwest::get(format!(
        "{big_central_url}/api/v1/revoked_tokens?token_num={token_num}&page_size={page_size}"
    ))
    .await?;

    if resp.status() != StatusCode::OK {
        return Err(anyhow!("{}", resp.text().await?));
    }

    let tokens: Vec<String> = resp.json().await?;
    let num_tokens: u32 = tokens.len().try_into()?;

    let revoked_tokens = &mut revoked_tokens.write().await;
    for token in tokens.into_iter() {
        let token: RevocationIdentiifer = hex::decode(token)?;
        revoked_tokens.insert(token);
    }

    Ok(num_tokens)
}

fn big_central_url() -> String {
    env::var("BIG_CENTRAL_URL").unwrap_or_else(|_| "https://bbfs.io".to_string())
}
