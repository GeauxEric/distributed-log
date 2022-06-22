fn main() {
    tonic_build::configure()
        .out_dir("protos/src")
        .compile(&["protos/api/v1/log.proto"], &["protos"])
        .expect("failed to compile protos");
}
