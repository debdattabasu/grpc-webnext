use std::path::PathBuf;

// The conformance contract lives outside this package (it is language-neutral and
// shared), and cargo's default "rerun if anything in the package changed" heuristic
// does not watch those files. Without these lines a change to `conformance.proto`
// leaves this driver compiled against last build's contract — which surfaces as the
// suite passing against a stale definition. That has bitten this repo once already
// (the Rust conformance server, 2026-07-27). Declare every input explicitly.
const PROTO: &str = "../../../conformance/proto/conformance.proto";
const PROTO_DIR: &str = "../../../conformance/proto";
// `google/api/annotations.proto` is vendored once at the shared proto root; the
// driver never uses the annotations, but the file imports them so they must resolve.
const SHARED_PROTO_DIR: &str = "../../../proto";

fn main() {
    println!("cargo:rerun-if-changed={PROTO}");
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/annotations.proto");
    println!("cargo:rerun-if-changed={SHARED_PROTO_DIR}/google/api/http.proto");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    // Messages only — no tonic, no generated service stubs. The client dispatches on
    // a method path string and deals in message bytes, so that is all it needs, and
    // keeping `prost-build` alone here mirrors the client's own no-tonic constraint.
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&[PROTO], &[PROTO_DIR, SHARED_PROTO_DIR])
        .expect("failed to compile conformance.proto");
}
