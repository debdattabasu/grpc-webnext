// Command conformance-server is the Go entry into the cross-language conformance
// matrix.
//
// It serves grpc.webnext.conformance.v1.ConformanceService over grpc-webnext —
// Fetch, WebSocket, h2ts, and native gRPC, all on one port — so the
// language-neutral driver can run the declarative cases in
// /conformance/cases/*.yaml against the Go implementation exactly as it does
// against the Rust one.
//
// Configuration comes from the env vars the harness sets per server profile (see
// /conformance/README.md "Server config profiles"); readiness is the
// `LISTENING http://<addr>` line on stdout.
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"google.golang.org/grpc"

	"github.com/grpc-webnext/grpc-webnext/go/internal/conformance"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

func main() {
	grpcServer := grpc.NewServer()
	conformance.Register(grpcServer)

	var cfg webnext.ServerConfig

	if v := os.Getenv("CONFORMANCE_MAX_MESSAGE_BYTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("CONFORMANCE_MAX_MESSAGE_BYTES: %v", err)
		}
		cfg.MaxMessageBytes = n
	}
	// CONFORMANCE_TRANSCODER=0 disables the +json path (the capability-gap case).
	if os.Getenv("CONFORMANCE_TRANSCODER") != "0" {
		fds, err := conformance.DescriptorSet()
		if err != nil {
			log.Fatalf("descriptor set: %v", err)
		}
		if cfg.Transcoder, err = webnext.NewTranscoder(fds); err != nil {
			log.Fatalf("transcoder: %v", err)
		}
	}

	addr, run, err := webnext.BindAndServe("127.0.0.1:0", grpcServer, cfg)
	if err != nil {
		log.Fatal(err)
	}
	// Readiness line parsed by the harness / conformance runner.
	fmt.Printf("LISTENING http://%s\n", addr)
	_ = os.Stdout.Sync()
	log.Fatal(run())
}
