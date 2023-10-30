/// Billy's file sync protocol
#[cfg(test)]
mod test;

#[cfg(feature = "file")]
pub mod crypto;
#[cfg(feature = "file")]
pub mod file;
#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "auth")]
pub mod auth;
