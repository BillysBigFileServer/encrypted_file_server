use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use macaroon::{Macaroon, MacaroonKey};
use rand::rngs::OsRng;
use serde::de::IntoDeserializer;
use serde_derive::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[tokio::main]
async fn main() -> Result<()> {
    macaroon::initialize().unwrap();
    //FIXME: please don't hard code the authentication key
    let key = Arc::new(MacaroonKey::generate(b"key"));

    let pool = Arc::new(
        sqlx::SqlitePool::connect(
            &env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./data.db".to_string()),
        )
        .await?,
    );

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/create_user", post(create_user))
        .route("/login_user", post(login))
        .with_state(pool)
        .with_state(key);

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CreateUserRequest {
    email: String,
    username: String,
    password: String,
}

impl CreateUserRequest {
    /// Returns true if the email is valid, false otherwise
    fn validate_email() -> bool {
        //FIXME
        true
    }
}

async fn create_user(
    State(pool): State<Arc<SqlitePool>>,
    Json(req): Json<CreateUserRequest>,
) -> String {
    let argon2 = Argon2::default();
    let salt: SaltString = SaltString::generate(&mut OsRng);
    let hashed_password = match argon2.hash_password(req.password.as_bytes(), &salt) {
        Ok(password) => password,
        Err(err) => {
            //TODO: log errors
            return "Internal server error".to_string();
        }
    };

    sqlx::query("insert into users ( username, email, password, salt ) values ( ?, ?, ?, ? )")
        .bind(&req.username)
        .bind(req.email)
        .bind(hashed_password.to_string())
        .bind(salt.to_string())
        .execute(pool.as_ref()).await.unwrap();

    "Registered user".to_string()
}

#[derive(Serialize, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login(
    State(pool): State<Arc<SqlitePool>>,
    Json(login_request): Json<LoginRequest>,
) -> String {
    println!("Received login reqest");

    let password: Option<String> = sqlx::query("select password from users where username = ?")
        .bind(&login_request.username)
        .fetch_optional(pool.as_ref())
        .await
        .unwrap()
        .map(|row| row.get::<String, _>("password"));

    let password_hash = match password {
        Some(password) => password,
        None => {
            return "Username or password is incorrect".to_string();
        }
    };

    let password_hash = PasswordHash::new(&password_hash).unwrap();
    if Argon2::default()
        .verify_password(login_request.password.as_bytes(), &password_hash)
        .is_err()
    {
        return "Username or password is incorrect".to_string();
    }

    let mut macaroon = match Macaroon::create(
        None,
        &MacaroonKey::generate(b"key"),
        format!("{}-{}", login_request.username, rand::random::<u64>()).into(),
    ) {
        Ok(macaroon) => macaroon,
        //FIXME: deal with this
        Err(error) => panic!("Error creating macaroon: {:?}", error),
    };

    macaroon.add_first_party_caveat(format!("username = {}", login_request.username).into());

    macaroon.serialize(macaroon::Format::V2).unwrap()
}
