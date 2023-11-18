/// Billy's file sync protocol
#[cfg(test)]
mod test;

pub(crate) mod bfsp {
    pub(crate) mod files {
        include!(concat!(env!("OUT_DIR"), "/bfsp.files.rs"));
    }
}

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(feature = "file")]
pub mod crypto;
#[cfg(feature = "file")]
pub mod file;
#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "auth")]
pub mod auth;
