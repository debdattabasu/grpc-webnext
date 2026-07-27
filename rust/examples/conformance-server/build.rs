use std::path::PathBuf;

// The protos this crate compiles live OUTSIDE it (the conformance contract is
// language-neutral and shared), and cargo's default "rerun if anything in the
// package changed" heuristic does not see them. Without these lines a change to
// `conformance.proto` leaves this server running last build's descriptor set —
// which surfaces as the conformance suite passing against a stale contract, or as
// REST routes that mysteriously 415. Declare every input explicitly.
const PROTO: &str = "../../../conformance/proto/conformance.proto";
const PROTO_DIR: &str = "../../../conformance/proto";
// `google/api/annotations.proto` is vendored once at the shared proto root and
// included from there, so the REST annotations have exactly one source.
const SHARED_PROTO_DIR: &str = "../../../proto";

fn main() {
    println!("cargo:rerun-if-changed={PROTO}");
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/annotations.proto");
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/http.proto");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(out.join("conformance_descriptor.bin"))
        .compile_protos(&[PROTO], &[PROTO_DIR, SHARED_PROTO_DIR])
        .expect("failed to compile conformance.proto");
}
