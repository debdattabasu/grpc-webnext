// grpc-webnext Go implementation (in-process server + wire codec).
//
// This module lives in a subdirectory of the polyglot monorepo, so its import
// path carries the `/go` suffix and its release tags are prefixed `go/vX.Y.Z`
// (e.g. `go/v0.1.0`). See ../README.md "Releasing".
module github.com/grpc-webnext/grpc-webnext/go

go 1.25.0

require (
	github.com/debdattabasu/h2ts/go v0.0.0-20260727014224-86a0999b343e
	github.com/gorilla/websocket v1.5.3
	golang.org/x/net v0.57.0
	google.golang.org/genproto/googleapis/api v0.0.0-20260724162435-b2f20204f0df
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260720155508-bb71a54f79dc // indirect
)
