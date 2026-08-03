fn main() {
    // The proto lives outside this package, where cargo's default "rerun if the
    // package changed" heuristic does not look — without this an edit to
    // `greeter.proto` would leave last build's stubs compiled in.
    println!("cargo:rerun-if-changed=../greeter.proto");

    // The client stubs are the point of the example — *tonic's own codegen*,
    // unmodified, running over the tunnel. The server half is generated too, but only
    // the tests use it: they stand up a real Greeter in-process and drive it through
    // these very stubs, so both ends of every cardinality are the generated code.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The one setting a grpc-webnext (or any wasm) client needs. By default the
        // stub also gets a `connect()` constructor returning
        // `Self<tonic::transport::Channel>` — hyper, TCP and TLS, none of which exist
        // in a browser and none of which even compile for wasm32. Turning it off
        // leaves `GreeterClient::new(T)`, which is the constructor that matters here:
        // the transport is passed in, and ours is the tunnel.
        .build_transport(false)
        .compile_protos(&["../greeter.proto"], &[".."])
        .expect("failed to compile greeter.proto");
}
