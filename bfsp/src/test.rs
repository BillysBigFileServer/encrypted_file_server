use anyhow::Result;
use tokio::fs;

use crate::cli::FileHeader;

//FIXME
/*
#[tokio::test]
async fn file_header_medium_file() -> Result<()> {
    let mut tux_file = fs::File::open("./test_files/tux_huge.png").await?;
    let file_header = FileHeader::from_file(&mut tux_file, None).await?;

    let chunks: HashMap<ChunkID, ChunkMetadata> = [
                (
                    0,
                    ChunkMetadata {
                        id: 0,
                        hash: "b2262595c40fcc22b7056107b4f744e01cca095f0cc4b0965d261396a370720a"
                            .to_string(),
                        size: 65536
                    }
                ),
                (
                    5,
                    ChunkMetadata {
                        id: 5,
                        hash: "1828f9fcd28d182d27af0383532c110db97a180a7b5b731aa814cc21bbf21513"
                            .to_string(),
                        size: 65536
                    }
                ),
                (
                    1,
                    ChunkMetadata {
                        id: 1,
                        hash: "4a0bf35c7cd77c033caf0d4f5be37e888d40a4143a4cd21ae3d7e640646b06dc"
                            .to_string(),
                        size: 65536
                    }
                ),
                (
                    6,
                    ChunkMetadata {
                        id: 6,
                        hash: "1fcc2640c103261fac764166ad9f6a38387d48b539883071beee69d8b032c762"
                            .to_string(),
                        size: 2964
                    }
                ),
                (
                    2,
                    ChunkMetadata {
                        id: 2,
                        hash: "60c225e18cfd5b83a08e61896d2f6f8a760de1645a82118c3cc71012b7a857f7"
                            .to_string(),
                        size: 65536
                    }
                ),
                (
                    3,
                    ChunkMetadata {
                        id: 3,
                        hash: "143e04b49e151902492b93f2aa25a5306ba17e2ef5da129e072aae16aabf99d2"
                            .to_string(),
                        size: 65536
                    }
                ),
                (
                    4,
                    ChunkMetadata {
                        id: 4,
                        hash: "f6897e05c8a3929bbf2b64a2003c43dd26c65cd2d639f8d9938145dc5c06efcc"
                            .to_string(),
                        size: 65536
                    }
                )
            ].into();

    let chunk_size = 65536;

    assert_eq!(file_header.chunk_size, chunk_size);
    assert_eq!(file_header.chunks, chunks);


    Ok(())
}
*/

#[tokio::test]
async fn test_consistent_file_headers() -> Result<()> {
    let mut tux_file = fs::File::open("./test_files/tux_huge.png").await?;
    let file_header = FileHeader::from_file(&mut tux_file).await?;
    let file_header2 = FileHeader::from_file(&mut tux_file).await?;

    assert_eq!(file_header, file_header2);

    Ok(())
}
