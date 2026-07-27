// Command greeter serves the shared Greeter demo service over grpc-webnext.
//
// It is the Go counterpart of rust/examples/greeter-server: the same service, on
// the same wire, so the TypeScript client's demo (node/packages/client/examples)
// drives either one interchangeably. Every RPC cardinality is covered — unary
// over Fetch, server/client/bidi streaming over WebSocket — plus native gRPC and
// the real-HTTP/2 h2ts tunnel, all on one port.
//
//	go run ./examples/greeter            # ephemeral port
//	go run ./examples/greeter 127.0.0.1:8080
//
// It also shows the shutdown shape: SIGINT drains — new RPCs are refused, in-flight
// ones finish — bounded by a deadline, then the process exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/grpc-webnext/grpc-webnext/go/examples/greeter/greeterpb"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

type greeter struct {
	pb.UnimplementedGreeterServer
}

func (greeter) SayHello(_ context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello, " + req.GetName() + "!"}, nil
}

// Sleep is deliberately slow, for exercising deadlines from a client.
func (greeter) Sleep(ctx context.Context, req *pb.SleepRequest) (*pb.HelloReply, error) {
	select {
	case <-time.After(time.Duration(req.GetMillis()) * time.Millisecond):
		return &pb.HelloReply{Message: "awake"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (greeter) Countdown(req *pb.CountdownRequest, stream grpc.ServerStreamingServer[pb.Tick]) error {
	for value := int64(req.GetFrom()); value >= 0; value-- {
		if err := stream.Send(&pb.Tick{Value: uint32(value)}); err != nil {
			return err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	return nil
}

func (greeter) Chat(stream grpc.BidiStreamingServer[pb.ChatMessage, pb.ChatMessage]) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.ChatMessage{Text: "echo: " + msg.GetText()}); err != nil {
			return err
		}
	}
}

func (greeter) Concat(stream grpc.ClientStreamingServer[pb.ChatMessage, pb.HelloReply]) error {
	var parts []string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.HelloReply{Message: strings.Join(parts, " ")})
		}
		if err != nil {
			return err
		}
		parts = append(parts, msg.GetText())
	}
}

func main() {
	addr := "127.0.0.1:0"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	// A *grpc.Server is already an http.Handler, so it is the backend directly.
	grpcServer := grpc.NewServer()
	pb.RegisterGreeterServer(grpcServer, greeter{})

	srv := webnext.NewServer(grpcServer, webnext.ServerConfig{
		// Ping an otherwise-quiet stream so an idle-timeout proxy cannot drop it.
		WSKeepalive: 30 * time.Second,
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	// The readiness convention the examples and the conformance harness share.
	fmt.Printf("LISTENING http://%s\n", listener.Addr())
	_ = os.Stdout.Sync()

	serve := make(chan error, 1)
	go func() { serve <- srv.Serve(listener) }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serve:
		log.Fatal(err)
	case sig := <-signals:
		log.Printf("%v: draining…", sig)
	}

	// Refuse new RPCs, let the in-flight ones finish — but do not wait forever on a
	// long-lived stream, because something upstream will not wait either.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("drain incomplete: %v", err)
	}
	if err := <-serve; err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	log.Print("stopped")
}
