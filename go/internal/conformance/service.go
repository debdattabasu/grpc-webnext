// Package conformance implements grpc.webnext.conformance.v1.ConformanceService,
// the one fixed service every grpc-webnext server implementation serves for the
// cross-language conformance matrix.
//
// The *request* carries a ResponseDefinition telling the server exactly how to
// respond (payload, status, metadata, timing, oversize), so this single generic
// service exercises every protocol feature with no per-case server code. It
// lives here rather than in the command so the package's own end-to-end tests
// can serve it too. See /conformance/README.md.
package conformance

import (
	"context"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/grpc-webnext/grpc-webnext/go/internal/conformance/conformancepb"
	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

// Register installs the conformance service on a gRPC server.
func Register(s grpc.ServiceRegistrar) { pb.RegisterConformanceServiceServer(s, conformanceServer{}) }

// DescriptorSet is the encoded FileDescriptorSet the `+json` transcoder needs,
// built straight from the descriptors compiled into the generated package — so
// there is no side-car .bin to keep in sync with the proto.
func DescriptorSet() ([]byte, error) {
	fdp := protodesc.ToFileDescriptorProto(pb.File_conformance_proto)
	return proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fdp}})
}

// --- metadata / request-info mapping ----------------------------------------

// toConformanceMetadata maps incoming gRPC metadata to the conformance
// Metadatum list. grpc-go already base64-decodes `-bin` values, so a binary
// entry's string holds the raw bytes. Framing headers are filtered with
// grpc-webnext's own denylist, so the echoed request_headers carry only what the
// client actually attached.
func toConformanceMetadata(md metadata.MD) []*pb.Metadatum {
	var out []*pb.Metadatum
	for key, values := range md {
		if webnext.IsDeniedHeader(key) {
			continue
		}
		for _, v := range values {
			if strings.HasSuffix(key, "-bin") {
				out = append(out, &pb.Metadatum{Key: key, Value: &pb.Metadatum_BinValue{BinValue: []byte(v)}})
			} else {
				out = append(out, &pb.Metadatum{Key: key, Value: &pb.Metadatum_AsciiValue{AsciiValue: v}})
			}
		}
	}
	return out
}

// fromConformanceMetadata maps a conformance Metadatum list back into gRPC
// metadata for emitting response headers / trailers.
func fromConformanceMetadata(items []*pb.Metadatum) metadata.MD {
	md := metadata.MD{}
	for _, m := range items {
		switch v := m.GetValue().(type) {
		case *pb.Metadatum_AsciiValue:
			md.Append(m.GetKey(), v.AsciiValue)
		case *pb.Metadatum_BinValue:
			md.Append(m.GetKey(), string(v.BinValue))
		}
	}
	return md
}

// requestInfo is the observed request context, echoed back for assertions.
//
// `json` is always false: an in-process service cannot see which codec
// terminated at the grpc-webnext edge — the same limitation the Rust server has.
// The timeout is read from the context deadline rather than the `grpc-timeout`
// header, which grpc-go strips before metadata reaches a service.
func requestInfo(ctx context.Context) *pb.RequestInfo {
	md, _ := metadata.FromIncomingContext(ctx)
	info := &pb.RequestInfo{RequestHeaders: toConformanceMetadata(md)}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline).Milliseconds(); remaining > 0 {
			info.TimeoutMillis = uint32(remaining)
		}
	}
	return info
}

// --- response construction ---------------------------------------------------

// buildPayload is the unary/echo payload: fabricated zeros when
// oversize_response_bytes is set (the max-message-size cases), else the
// definition's payload, else the request's.
func buildPayload(rd *pb.ResponseDefinition, requestPayload []byte) []byte {
	if n := rd.GetOversizeResponseBytes(); n > 0 {
		return make([]byte, n)
	}
	if p := rd.GetPayload(); len(p) > 0 {
		return p
	}
	return requestPayload
}

// delay sleeps for delayMs, aborting early if the call is cancelled — which is
// exactly what the deadline cases exercise.
func delay(ctx context.Context, delayMs uint32) error {
	if delayMs == 0 {
		return nil
	}
	t := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func statusFrom(rd *pb.ResponseDefinition) error {
	return status.Error(codes.Code(rd.GetStatusCode()), rd.GetStatusMessage())
}

// --- service -----------------------------------------------------------------

type conformanceServer struct {
	pb.UnimplementedConformanceServiceServer
}

func (conformanceServer) Unary(ctx context.Context, req *pb.UnaryRequest) (*pb.ConformancePayload, error) {
	info := requestInfo(ctx)
	rd := req.GetResponseDefinition()

	if err := delay(ctx, rd.GetDelayMs()); err != nil {
		return nil, err
	}
	if rd.GetStatusCode() != 0 {
		if md := fromConformanceMetadata(rd.GetTrailers()); md.Len() > 0 {
			_ = grpc.SetTrailer(ctx, md)
		}
		return nil, statusFrom(rd)
	}
	if md := fromConformanceMetadata(rd.GetHeaders()); md.Len() > 0 {
		_ = grpc.SetHeader(ctx, md)
	}
	return &pb.ConformancePayload{
		Payload:     buildPayload(rd, req.GetPayload()),
		RequestInfo: info,
	}, nil
}

func (conformanceServer) ServerStream(req *pb.ServerStreamRequest, stream pb.ConformanceService_ServerStreamServer) error {
	ctx := stream.Context()
	info := requestInfo(ctx)
	rd := req.GetResponseDefinition()

	if md := fromConformanceMetadata(rd.GetHeaders()); md.Len() > 0 {
		_ = stream.SetHeader(md)
	}
	first := true
	for _, sm := range rd.GetStreamMessages() {
		if err := delay(ctx, sm.GetDelayMs()); err != nil {
			return err
		}
		payload := &pb.ConformancePayload{Payload: sm.GetPayload()}
		if first {
			payload.RequestInfo, first = info, false
		}
		if err := stream.Send(payload); err != nil {
			return err
		}
	}
	if rd.GetStatusCode() != 0 {
		stream.SetTrailer(fromConformanceMetadata(rd.GetTrailers()))
		return statusFrom(rd)
	}
	return nil
}

func (conformanceServer) ClientStream(stream pb.ConformanceService_ClientStreamServer) error {
	ctx := stream.Context()
	info := requestInfo(ctx)

	var rd *pb.ResponseDefinition
	var count uint32
	var total uint64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if count == 0 {
			rd = msg.GetResponseDefinition() // honored on the first request only
		}
		count++
		total += uint64(len(msg.GetPayload()))
	}

	if err := delay(ctx, rd.GetDelayMs()); err != nil {
		return err
	}
	if rd.GetStatusCode() != 0 {
		stream.SetTrailer(fromConformanceMetadata(rd.GetTrailers()))
		return statusFrom(rd)
	}
	if md := fromConformanceMetadata(rd.GetHeaders()); md.Len() > 0 {
		_ = stream.SetHeader(md)
	}
	return stream.SendAndClose(&pb.ClientStreamResponse{
		Payload:       &pb.ConformancePayload{Payload: rd.GetPayload(), RequestInfo: info},
		ReceivedCount: count,
		ReceivedBytes: total,
	})
}

func (conformanceServer) BidiStream(stream pb.ConformanceService_BidiStreamServer) error {
	info := requestInfo(stream.Context())

	var rd *pb.ResponseDefinition
	first := true
	for {
		// A client Reset surfaces here as a cancellation; propagate it as the
		// stream's terminal status and stop.
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		payload := &pb.ConformancePayload{Payload: msg.GetPayload()}
		if first {
			rd, payload.RequestInfo, first = msg.GetResponseDefinition(), info, false
		}
		if err := stream.Send(payload); err != nil {
			return err
		}
	}
	if rd.GetStatusCode() != 0 {
		stream.SetTrailer(fromConformanceMetadata(rd.GetTrailers()))
		return statusFrom(rd)
	}
	return nil
}
