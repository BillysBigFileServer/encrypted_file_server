// TODO create an AuthorizedToken struct so that type safety protects us from fucking up
use anyhow::anyhow;
use std::{collections::BTreeSet, fmt::Display, time::Duration};

use biscuit_auth::{
    builder::{set, string},
    datalog::RunLimits,
    macros::authorizer,
    Biscuit,
};
use tracing::{event, Level};

use crate::{meta_db::MetaDB, tokens::check_token_revoked};

#[derive(Debug)]
pub enum Right {
    Read,
    Query,
    Write,
    Delete,
    Usage,
    Payment,
    Settings,
}

impl Right {
    pub fn to_str(&self) -> &'static str {
        match self {
            Right::Read => "read",
            Right::Query => "query",
            Right::Write => "write",
            Right::Delete => "delete",
            Right::Usage => "usage",
            Right::Payment => "payment",
            Right::Settings => "settings",
        }
    }
}

#[tracing::instrument(err, skip(token))]
pub async fn authorize<M: MetaDB>(
    right_being_checked: Right,
    token: &Biscuit,
    file_ids: Vec<String>,
    meta_db: &M,
) -> anyhow::Result<i64> {
    if check_token_revoked(token).await {
        return Err(anyhow!("token is revoked"));
    }

    let user_id = get_user_id(token)?;

    // first, check if the user has been suspended from the right they're trying to execute
    let suspensions = meta_db.suspensions(&[user_id]).await?;
    let suspension = suspensions.get(&user_id).unwrap();

    let can_perform_action = match right_being_checked {
        Right::Read => !suspension.read_suspended,
        Right::Write => !suspension.write_suspended,
        Right::Delete => !suspension.delete_suspended,
        Right::Query => !suspension.query_suspended,
        _ => true,
    };

    if !can_perform_action {
        return Err(anyhow!(
            "suspended from performing action {}",
            right_being_checked.to_str()
        ));
    }

    let mut authorizer = authorizer!(
        r#"
            check if user($user);
            check if rights($rights), right($right), $rights.contains($right);

            allow if true;
            deny if false;
        "#
    );
    authorizer.add_token(token).unwrap();

    authorizer
        .add_fact(biscuit_auth::builder::fact(
            "right",
            &[string(right_being_checked.to_str())],
        ))
        .unwrap();

    let auth_file_ids = set(file_ids
        .iter()
        .map(|id| string(id))
        .collect::<BTreeSet<_>>());

    authorizer
        .add_fact(biscuit_auth::builder::fact(
            "file_ids",
            &[auth_file_ids.clone()],
        ))
        .unwrap();

    authorizer.authorize().unwrap();

    Ok(user_id)
}

#[derive(thiserror::Error, Debug)]
pub enum GetUserIDError {
    MultipleUserIDs,
}

impl Display for GetUserIDError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("multipe user ids")
    }
}

#[tracing::instrument(err, skip(token))]
fn get_user_id(token: &Biscuit) -> Result<i64, GetUserIDError> {
    let mut authorizer = authorizer!(
        r#"
        check if user($user);

        allow if true;
        deny if false;
    "#
    );
    authorizer.add_token(token).unwrap();
    let user_info: Vec<(String,)> = authorizer
        .query_with_limits(
            "data($0) <- user($0)",
            RunLimits {
                max_time: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .unwrap();

    if user_info.len() != 1 {
        return Err(GetUserIDError::MultipleUserIDs);
    }

    let user_id: i64 = user_info.first().unwrap().0.parse().unwrap();
    event!(Level::INFO, user_id = user_id);
    Ok(user_id)
}
