use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let local_proto = manifest_dir.join("proto/vectordb.proto");
    let proto_path = if local_proto.exists() {
        local_proto
    } else {
        manifest_dir.join("../../proto/vectordb.proto")
    };
    let proto_dir = proto_path
        .parent()
        .ok_or("Proto file path has no parent directory")?;

    println!("cargo:rerun-if-changed={}", proto_path.display());

    tonic_prost_build::configure().compile_protos(&[proto_path.as_path()], &[proto_dir])?;
    Ok(())
}
