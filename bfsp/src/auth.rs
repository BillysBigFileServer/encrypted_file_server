use anyhow::{anyhow, Result};
use macaroon::ByteString;
use regex::Regex;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

#[derive(Clone)]
pub struct UsernameCaveat {
    pub username: String,
}

impl Into<ByteString> for UsernameCaveat {
    fn into(self) -> ByteString {
        self.to_string().into()
    }
}

impl ToString for UsernameCaveat {
    fn to_string(&self) -> String {
        format!("username = {}", self.username)
    }
}

impl TryFrom<ByteString> for UsernameCaveat {
    type Error = anyhow::Error;

    fn try_from(value: ByteString) -> Result<Self> {
        let value: String = String::from_utf8(value.0.clone())?;

        let re = Regex::new("username = (?<username>[a-zA-Z0-9_-]*)")?;
        let Some(caps) = re.captures(&value) else {
            return Err(anyhow!("invalid username caveat"));
        };
        let username = &caps["username"];

        Ok(Self {
            username: username.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct ExpirationCaveat {
    pub expiration: u64,
}

impl Into<ByteString> for ExpirationCaveat {
    fn into(self) -> ByteString {
        self.to_string().into()
    }
}

impl ToString for ExpirationCaveat {
    fn to_string(&self) -> String {
        format!("expires = {}", self.expiration)
    }
}

impl TryFrom<&ByteString> for ExpirationCaveat {
    type Error = anyhow::Error;

    fn try_from(value: &ByteString) -> Result<Self> {
        let value: String = String::from_utf8(value.0.clone())?;

        let re = Regex::new("expires = (?<expires>[a-zA-Z0-9_-]*)")?;
        let Some(caps) = re.captures(&value) else {
            return Err(anyhow!("invalid expiration caveat"));
        };
        let expiration = &caps["expires"].to_string();
        let expiration: u64 = expiration.parse()?;

        Ok(Self { expiration })
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Authentication {
    pub macaroon: String,
}

impl Authentication {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, 1024>(self)?;
        let mut buf: AlignedVec = {
            let mut buf = AlignedVec::with_capacity(2);
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf
        };
        buf.extend_from_slice(&bytes);

        Ok(buf)
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<&ArchivedAuthentication> {
        rkyv::check_archived_root::<Self>(bytes)
            .map_err(|_| anyhow!("Error deserializing PartsUploaded"))
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub username: String,
    pub password: String,
}

impl CreateUserRequest {
    /// Returns true if the email is valid, false otherwise
    fn validate_email() -> bool {
        //FIXME
        true
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}
