// Package protoset builds an encoded FileDescriptorSet from descriptors already
// compiled into a generated package — the `protoc --descriptor_set_out
// --include_imports` blob a grpc-webnext `Transcoder` wants, without a side-car
// file to keep in sync.
//
// The `--include_imports` half is the part that matters and the part that is easy
// to get wrong: a proto with imports (`google/api/annotations.proto`, say) yields
// a set that fails to build a descriptor pool unless its whole import closure is
// in there too, and the failure surfaces far from the cause — as "no such method"
// or, worse, as REST routes that silently never match.
package protoset

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Marshal encodes `root` and its full import closure as a FileDescriptorSet.
func Marshal(root protoreflect.FileDescriptor) ([]byte, error) {
	var files []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}

	// Post-order, because a file's dependencies must precede it in the set.
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
		files = append(files, protodesc.ToFileDescriptorProto(fd))
	}
	add(root)

	return proto.Marshal(&descriptorpb.FileDescriptorSet{File: files})
}
