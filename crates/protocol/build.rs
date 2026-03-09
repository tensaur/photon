fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "src/codec/protobuf/proto/ingest.proto",
                "src/codec/protobuf/proto/query.proto",
            ],
            &["src/codec/protobuf/proto"],
        )?;
    Ok(())
}
