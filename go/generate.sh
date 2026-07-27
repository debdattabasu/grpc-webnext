#!/usr/bin/env bash
# Regenerate the Go bindings that are CHECKED IN to this module.
#
# The `.proto` files at the repo root are the shared contract and the only place to
# edit them (see /CLAUDE.md); each language generates its own bindings. Go, like Node,
# checks the generated code in — the proto is a dev-time codegen input, so the module
# never ships a `.proto` and `go build` needs no protoc.
#
# The Go import path for each proto is supplied here with `M<file>=<import path>`
# rather than a `go_package` option, so the shared contract stays language-neutral.
#
#   go/webnext/pb          <- /proto/grpc_webnext.proto            (the wire envelope)
#   go/internal/conformance/conformancepb
#                          <- /conformance/proto/conformance.proto (the test service)
#   go/examples/greeter/greeterpb
#                          <- /rust/examples/greeter.proto         (the demo service)
#
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail

cd "$(dirname "$0")"
root=".."
mod="github.com/grpc-webnext/grpc-webnext/go"

# --- the wire envelope (messages only; it declares no service) ---------------
rm -f webnext/pb/*.pb.go
protoc \
  -I "$root/proto" \
  --go_out=. --go_opt=module="$mod" \
  --go_opt=Mgrpc_webnext.proto="$mod/webnext/pb" \
  "$root/proto/grpc_webnext.proto"

# --- the conformance service (messages + grpc-go service stubs) -------------
rm -f internal/conformance/conformancepb/*.pb.go
protoc \
  -I "$root/conformance/proto" \
  --go_out=. --go_opt=module="$mod" \
  --go_opt=Mconformance.proto="$mod/internal/conformance/conformancepb" \
  --go-grpc_out=. --go-grpc_opt=module="$mod" \
  --go-grpc_opt=Mconformance.proto="$mod/internal/conformance/conformancepb" \
  "$root/conformance/proto/conformance.proto"

# --- the greeter demo service (shared with the Rust and TS examples) --------
rm -f examples/greeter/greeterpb/*.pb.go
protoc \
  -I "$root/rust/examples" \
  --go_out=. --go_opt=module="$mod" \
  --go_opt=Mgreeter.proto="$mod/examples/greeter/greeterpb" \
  --go-grpc_out=. --go-grpc_opt=module="$mod" \
  --go-grpc_opt=Mgreeter.proto="$mod/examples/greeter/greeterpb" \
  "$root/rust/examples/greeter.proto"

gofmt -w webnext/pb internal/conformance/conformancepb examples/greeter/greeterpb
echo "generated: webnext/pb, internal/conformance/conformancepb, examples/greeter/greeterpb"
