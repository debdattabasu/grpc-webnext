// Package testecho is the Go implementation of the Echo service used by the
// Rust crate's transcoding tests.
//
// It exists so both servers are held to the SAME `google.api.http` annotations
// (`/rust/crates/testecho/proto/echo.proto` is the single source), rather than
// each implementation inventing its own REST fixtures and drifting. The Go REST
// tests in `webnext` are deliberate ports of `rust/.../tests/inproc_json.rs`,
// asserting the same URLs against the same service.
//
// Test-only, hence `internal/`: nothing here ships to users.
package testecho

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grpc-webnext/grpc-webnext/go/internal/protoset"
	pb "github.com/grpc-webnext/grpc-webnext/go/internal/testecho/testechopb"
)

// Service implements echo.v1.Echo.
type Service struct {
	pb.UnimplementedEchoServer

	// flakyRemaining is how many more times FlakyUnary fails before it starts
	// succeeding; 0 (the zero value) means it always succeeds.
	flakyRemaining atomic.Int32
}

// New builds a Service whose FlakyUnary fails `failTimes` times before it starts
// succeeding — mirroring the Rust `EchoSvc::flaky`. Zero means always succeed.
func New(failTimes int32) *Service {
	s := &Service{}
	s.flakyRemaining.Store(failTimes)
	return s
}

// Register installs the service on a gRPC server.
func Register(s grpc.ServiceRegistrar) { pb.RegisterEchoServer(s, New(0)) }

// DescriptorSet is the encoded FileDescriptorSet the `+json` transcoder and the
// HTTP router are built from — assembled from the descriptors compiled into the
// generated package, so no side-car `.bin` file is needed. echo.proto has imports
// (`google/api/…`), so the whole closure goes in; see the protoset package.
func DescriptorSet() ([]byte, error) {
	return protoset.Marshal(pb.File_echo_proto)
}

func (*Service) Unary(_ context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	return &pb.EchoResponse{Message: req.GetMessage()}, nil
}

func (*Service) Sleep(ctx context.Context, req *pb.SleepRequest) (*pb.EchoResponse, error) {
	if millis := req.GetMillis(); millis > 0 {
		select {
		case <-time.After(time.Duration(millis) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &pb.EchoResponse{Message: "awake"}, nil
}

// FlakyUnary fails with UNAVAILABLE while failures remain, then echoes. It is
// the only *un-annotated* unary method, which is also what makes it the fixture
// for "a plain-JSON URL matching no REST binding falls back to the main path".
func (s *Service) FlakyUnary(_ context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	for {
		remaining := s.flakyRemaining.Load()
		if remaining <= 0 {
			return &pb.EchoResponse{Message: req.GetMessage()}, nil
		}
		if s.flakyRemaining.CompareAndSwap(remaining, remaining-1) {
			return nil, status.Error(codes.Unavailable, "flaky: transient failure")
		}
	}
}

func (*Service) Stream(stream grpc.BidiStreamingServer[pb.EchoRequest, pb.EchoResponse]) error {
	return echoLoop(stream)
}

func (*Service) Chat(stream grpc.BidiStreamingServer[pb.EchoRequest, pb.EchoResponse]) error {
	return echoLoop(stream)
}

func (*Service) Repeat(req *pb.RepeatRequest, stream grpc.ServerStreamingServer[pb.EchoResponse]) error {
	for i := uint32(0); i < req.GetCount(); i++ {
		if err := stream.Send(&pb.EchoResponse{Message: req.GetMessage()}); err != nil {
			return err
		}
	}
	return nil
}

// Hang emits one message then blocks, so a test can observe upstream
// cancellation when the client resets or disconnects.
func (*Service) Hang(_ *pb.EchoRequest, stream grpc.ServerStreamingServer[pb.EchoResponse]) error {
	if err := stream.Send(&pb.EchoResponse{Message: "started"}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func echoLoop(stream grpc.BidiStreamingServer[pb.EchoRequest, pb.EchoResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.EchoResponse{Message: req.GetMessage()}); err != nil {
			return err
		}
	}
}
