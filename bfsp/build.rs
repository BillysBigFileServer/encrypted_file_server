use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/bfsp.proto"], &["src/"])?;
    Ok(())
}
