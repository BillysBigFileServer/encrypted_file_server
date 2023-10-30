use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_macros::debug_handler;
use bfsp::{CreateUserRequest, LoginRequest, UsernameCaveat};
use macaroon::{Macaroon, MacaroonKey};
use rand::rngs::OsRng;
use sqlx::{Row, SqlitePool};

#[tokio::main]
async fn main() -> Result<()> {
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
        .level_for("sqlx", log::LevelFilter::Warn)
        .level_for("hyper", log::LevelFilter::Warn)
        // - and per-module overrides
        // Output to stdout, files, and other Dispatch configurations
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log").unwrap())
        // Apply globally
        .apply()
        .unwrap();

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

async fn create_user(
    State(pool): State<Arc<SqlitePool>>,
    Json(req): Json<CreateUserRequest>,
) -> (StatusCode, String) {
    let argon2 = Argon2::default();
    let salt: SaltString = SaltString::generate(&mut OsRng);
    let hashed_password = match argon2.hash_password(req.password.as_bytes(), &salt) {
        Ok(password) => password,
        Err(err) => {
            //TODO: log errors
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            );
        }
    };

    match sqlx::query("insert into users ( username, email, password, salt ) values ( ?, ?, ?, ? )")
        .bind(&req.username)
        .bind(req.email)
        .bind(hashed_password.to_string())
        .bind(salt.to_string())
        .execute(pool.as_ref())
        .await
    {
        Ok(_) => (StatusCode::OK, "registered user".to_string()),
        Err(err) => match err.into_database_error().unwrap().kind() {
            sqlx::error::ErrorKind::UniqueViolation => todo!(),
            sqlx::error::ErrorKind::ForeignKeyViolation => todo!(),
            sqlx::error::ErrorKind::NotNullViolation => todo!(),
            sqlx::error::ErrorKind::CheckViolation => todo!(),
            sqlx::error::ErrorKind::Other => todo!(),
            _ => todo!(),
        },
    }
}

async fn login(
    State(pool): State<Arc<SqlitePool>>,
    Json(login_request): Json<LoginRequest>,
) -> (StatusCode, String) {
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
            return (
                StatusCode::UNAUTHORIZED,
                "Username or password is incorrect".to_string(),
            );
        }
    };

    let password_hash = PasswordHash::new(&password_hash).unwrap();
    if Argon2::default()
        .verify_password(login_request.password.as_bytes(), &password_hash)
        .is_err()
    {
        return (
            StatusCode::UNAUTHORIZED,
            "Username or password is incorrect".to_string(),
        );
    }

    let key = MacaroonKey::generate(b"key");

    let mut macaroon = match Macaroon::create(
        None,
        &key,
        format!("{}-{}", login_request.username, rand::random::<u16>()).into(),
    ) {
        Ok(macaroon) => macaroon,
        //FIXME: deal with this
        Err(error) => panic!("Error creating macaroon: {:?}", error),
    };

    macaroon.add_first_party_caveat(
        UsernameCaveat {
            username: login_request.username,
        }
        .into(),
    );
    macaroon.add_first_party_caveat(
        format!(
            "expires = {}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )
        .into(),
    );

    (
        StatusCode::OK,
        macaroon.serialize(macaroon::Format::V2JSON).unwrap(),
    )
}
