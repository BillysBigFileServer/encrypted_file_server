use std::sync::Arc;
use std::time::Duration;
use std::{env, sync::atomic::AtomicBool};

use log::{error, info};
use rcgen::{Certificate, CertificateParams, DistinguishedName};

use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, ChallengeType, Identifier, NewOrder,
    OrderState, OrderStatus,
};
use sqlx::Row;
use tokio::sync::RwLock;

struct AcmeChallenge {
    pub token: String,
    pub text: String,
}

pub(crate) struct TlsCerts {
    pub(crate) cert_chain_pem: String,
    pub(crate) private_key_pem: String,
}

pub async fn get_tls_cert() -> anyhow::Result<TlsCerts> {
    let pool = sqlx::PgPool::connect(
        &env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/efs_db".to_string()),
    )
    .await?;

    Ok(sqlx::query("
        SELECT cert_chain_pem, private_key_pem
        FROM tls_certs
        WHERE expires_at > NOW()
        ORDER BY expires_at DESC
        LIMIT 1
    ").fetch_optional(&pool).await.map(|row| async move {
        match row {
            Some(row) => TlsCerts {
                cert_chain_pem: row.get("cert_chain_pem"),
                private_key_pem: row.get("private_key_pem"),
            },
            None => {
                let tls_certs = order_tls_certs().await.unwrap();

                // they really expire in 90 days, but let's encrypt recommends rotating every 60
                sqlx::query(
                    "INSERT INTO tls_certs (cert_chain_pem, private_key_pem, expires_at) VALUES ($1, $2, now() + INTERVAL '60 days')",
                )
                    .bind(&tls_certs.cert_chain_pem)
                    .bind(&tls_certs.private_key_pem)
                    .execute(&pool).await.unwrap();
                tls_certs
            }
        }
    }).unwrap().await)
}

async fn order_tls_certs() -> anyhow::Result<TlsCerts> {
    let acme_challenge: Arc<RwLock<Option<AcmeChallenge>>> = Arc::new(RwLock::new(None));

    let acme_challenge_clone = acme_challenge.clone();
    tokio::task::spawn(async move {
        http_server(acme_challenge_clone).await;
    });

    let acc_key = env::var("LETS_ENCRYPT_KEY").unwrap();
    let acc_creds = serde_json::from_str::<AccountCredentials>(acc_key.as_str()).unwrap();
    let account = Account::from_credentials(acc_creds).await.unwrap();

    let identifier = Identifier::Dns("big-file-server.fly.dev".to_string());
    let mut order = account
        .new_order(&NewOrder {
            identifiers: &[identifier],
        })
        .await
        .unwrap();

    let state = order.state();
    if ![OrderStatus::Pending, OrderStatus::Ready].contains(&state.status) {
        return Err(anyhow::anyhow!("order is not pending or ready"));
    }
    info!("order state: {:#?}", state);

    // Pick the desired challenge type and prepare the response.
    let authorizations = order.authorizations().await.unwrap();
    let mut challenges = Vec::with_capacity(authorizations.len());
    for authz in &authorizations {
        match authz.status {
            AuthorizationStatus::Pending => {}
            AuthorizationStatus::Valid => {}
            _ => todo!(),
        }

        let challenge = authz
            .challenges
            .iter()
            .find(|c| c.r#type == ChallengeType::Http01)
            .ok_or_else(|| anyhow::anyhow!("no http01 challenge found"))
            .unwrap();

        let Identifier::Dns(identifier) = &authz.identifier;
        acme_challenge.write().await.replace(AcmeChallenge {
            token: challenge.token.clone(),
            text: order.key_authorization(challenge).as_str().to_string(),
        });

        challenges.push((identifier, &challenge.url));
    }

    // Let the server know we're ready to accept the challenges.

    for (_, url) in challenges.iter() {
        order.set_challenge_ready(url).await.unwrap();
    }

    // Exponentially back off until the order becomes ready or invalid.

    let mut tries = 1u8;
    let mut delay = Duration::from_millis(250);
    let state = loop {
        tokio::time::sleep(delay).await;

        order.refresh().await.unwrap();
        let state = order.state();
        if let OrderStatus::Ready | OrderStatus::Invalid | OrderStatus::Valid = state.status {
            info!("order state: {:#?}", state);
            break state;
        }

        delay *= 2;
        tries += 1;
        match tries < 10 {
            true => info!("order is not ready, waiting {delay:?} {state:?} {tries}"),
            false => {
                error!("order is not ready {state:?} {tries}");
                return Err(anyhow::anyhow!(
                    "order is not ready. make sure the HTTP server is running and reachable"
                ));
            }
        }
    };

    if state.status == OrderStatus::Invalid {
        return Err(anyhow::anyhow!("order is invalid"));
    }

    let mut names = Vec::with_capacity(challenges.len());
    for (identifier, _) in challenges {
        names.push(identifier.to_owned());
    }

    // If the order is ready, we can provision the certificate.
    // Use the rcgen library to create a Certificate Signing Request.

    let mut params = CertificateParams::new(names.clone());
    params.distinguished_name = DistinguishedName::new();
    let cert = Certificate::from_params(params).unwrap();
    let csr = cert.serialize_request_der().unwrap();

    // Finalize the order and print certificate chain, private key and account credentials.

    order.finalize(&csr).await.unwrap();
    tries = 0;
    let cert_chain_pem = loop {
        match order.certificate().await.unwrap() {
            Some(cert_chain_pem) => break cert_chain_pem,
            None => tokio::time::sleep(Duration::from_secs(1)).await,
        }
        tries += 1;
        if tries > 10 {
            panic!("No cert received");
        }
    };

    Ok(TlsCerts {
        cert_chain_pem,
        private_key_pem: cert.serialize_private_key_pem(),
    })
}

static HTTP_SERVER_ACTIVE: AtomicBool = AtomicBool::new(false);

async fn http_server(acme_challenge: Arc<RwLock<Option<AcmeChallenge>>>) {
    if HTTP_SERVER_ACTIVE.load(std::sync::atomic::Ordering::SeqCst) {
        return;
    }

    HTTP_SERVER_ACTIVE.store(true, std::sync::atomic::Ordering::SeqCst);

    use warp::Filter;
    let acme =
        warp::path!(".well-known" / "acme-challenge" / String).then(move |challenge_token| {
            let acme_challenge = acme_challenge.clone();
            async move {
                info!("ACME challenge!");
                let challenge = acme_challenge.read().await;
                match &*challenge {
                    Some(challenge) if challenge.token == challenge_token => {
                        let response = warp::reply::Response::new(challenge.text.clone().into());
                        let (mut parts, body): (warp::http::response::Parts, _) =
                            response.into_parts();

                        parts.status = warp::http::status::StatusCode::OK;
                        warp::reply::Response::from_parts(parts, body)
                    }
                    _ => {
                        let response =
                            warp::reply::Response::new("404, stop visiting this page :)".into());
                        let (mut parts, body): (warp::http::response::Parts, _) =
                            response.into_parts();

                        parts.status = warp::http::status::StatusCode::NOT_FOUND;
                        warp::reply::Response::from_parts(parts, body)
                    }
                }
            }
        });

    warp::serve(acme).run(([0, 0, 0, 0], 80)).await;
}
