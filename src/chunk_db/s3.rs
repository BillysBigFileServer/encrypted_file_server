use bfsp::ChunkID;
use s3::{creds::Credentials, Bucket, Region};

use super::ChunkDB;

pub struct S3ChunkDB {
    bucket: Bucket,
}

impl ChunkDB for S3ChunkDB {
    fn new() -> anyhow::Result<Self> {
        let bucket_name = std::env::var("BUCKET_NAME")?;
        let region = Region::Custom {
            region: "auto".to_string(),
            endpoint: "https://fly.storage.tigris.dev".to_string(),
        };
        let creds = Credentials::default()?;
        let bucket = Bucket::new(bucket_name.as_str(), region, creds)?;

        Ok(Self { bucket })
    }

    async fn get_chunk(&self, chunk_id: &ChunkID, user_id: i64) -> anyhow::Result<Option<Vec<u8>>> {
        let path = S3ChunkDB::get_path(chunk_id, user_id).await;
        let resp = self.bucket.get_object(path.as_str()).await?;
        if [403, 404].contains(&resp.status_code()) {
            return Ok(None);
        }
        let body = resp.to_vec();

        Ok(Some(body))
    }

    async fn put_chunk(&self, chunk_id: &ChunkID, user_id: i64, data: &[u8]) -> anyhow::Result<()> {
        let path = S3ChunkDB::get_path(chunk_id, user_id).await;
        let resp = self.bucket.put_object(path.as_str(), data).await?;
        if resp.status_code() != 200 {
            return Err(anyhow::anyhow!(
                "Failed to put object: {:?}",
                resp.as_str().unwrap()
            ));
        }

        Ok(())
    }

    async fn delete_chunk(&self, chunk_id: &ChunkID, user_id: i64) -> anyhow::Result<()> {
        let path = S3ChunkDB::get_path(chunk_id, user_id).await;
        let resp = self.bucket.delete_object(path.as_str()).await?;
        if resp.status_code() != 204 {
            return Err(anyhow::anyhow!(
                "Failed to delete object: {:?}",
                resp.as_str().unwrap()
            ));
        }

        Ok(())
    }

    async fn get_path(chunk_id: &ChunkID, user_id: i64) -> String {
        format!("/{}/{}", user_id, chunk_id)
    }
}
