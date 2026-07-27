use std::path::PathBuf;

/// The vendored `google/api` subset lives at the shared proto root, outside this
/// package — so cargo's default "rerun if the package changed" heuristic misses it,
/// and an edit there would silently leave a stale descriptor set behind.
const SHARED_PROTO_DIR: &str = "../../../proto";

fn main() {
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/annotations.proto");
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/http.proto");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out.join("echo_descriptor.bin"))
        // `google/api/*.proto` is vendored once, at the repo's shared proto root, and
        // included from there by every consumer (this crate, the conformance protos,
        // and the Go/TS codegen) — one copy, so the annotations can't drift.
        .compile_protos(&["proto/echo.proto"], &["proto", SHARED_PROTO_DIR])
        .expect("failed to compile echo.proto");

    // A separate compile (no descriptor set) for a test-only server-reflection server
    // that returns raw descriptor bytes, so custom options like `google.api.http`
    // survive — unlike tonic-reflection, which round-trips through prost and strips them.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/reflection_v1.proto"], &["proto"])
        .expect("failed to compile reflection_v1.proto");
}
