// TODO create an AuthorizedToken struct so that type safety protects us from fucking up
use std::{collections::BTreeSet, fmt::Display, time::Duration};

use biscuit_auth::{
    builder::{set, string},
    datalog::RunLimits,
    macros::authorizer,
    Biscuit,
};
use tracing::{event, Level};

#[derive(Debug)]
pub enum Right {
    Read,
    Query,
    Write,
    Delete,
    Usage,
    Payment,
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
        }
    }
}

#[tracing::instrument(err, skip(token))]
pub fn authorize(
    right_being_checked: Right,
    token: &Biscuit,
    file_ids: Vec<String>,
    _chunk_ids: Vec<String>,
) -> anyhow::Result<()> {
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

    Ok(())
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
pub fn get_user_id(token: &Biscuit) -> Result<i64, GetUserIDError> {
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
