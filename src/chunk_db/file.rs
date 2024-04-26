use s3::Bucket;

use super::ChunkDB;

pub struct FSCHunkDB {
    bucket: Bucket,
}

impl ChunkDB for FSCHunkDB {
    fn new() -> anyhow::Result<Self> {
        todo!()
    }

    async fn get_chunk(
        &self,
        chunk_id: &bfsp::ChunkID,
        user_id: i64,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        todo!()
    }

    async fn put_chunk(
        &self,
        chunk_id: &bfsp::ChunkID,
        user_id: i64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_chunk(&self, chunk_id: &bfsp::ChunkID, user_id: i64) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_path(chunk_id: &bfsp::ChunkID, user_id: i64) -> String {
        todo!()
    }
}
