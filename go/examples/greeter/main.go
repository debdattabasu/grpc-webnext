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
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
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

	bound, run, err := webnext.BindAndServe(addr, grpcServer, webnext.ServerConfig{
		// Ping an otherwise-quiet stream so an idle-timeout proxy cannot drop it.
		WSKeepalive: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	// The readiness convention the examples and the conformance harness share.
	fmt.Printf("LISTENING http://%s\n", bound)
	_ = os.Stdout.Sync()
	log.Fatal(run())
}
