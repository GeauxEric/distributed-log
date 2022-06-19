fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["proto/api/v1/log.proto"], &["proto"])?;
    Ok(())
}
