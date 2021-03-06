// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.6.1
// source: InternalMessage.proto

package protobuf

import (
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	net "net"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type InternalMsg struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MessageID  []byte `protobuf:"bytes,1,opt,name=messageID,proto3" json:"messageID,omitempty"`
	Payload    []byte `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	CheckSum   uint64 `protobuf:"fixed64,3,opt,name=checkSum,proto3" json:"checkSum,omitempty"`
	InternalID uint32 `protobuf:"varint,4,opt,name=internalID,proto3" json:"internalID,omitempty"`
	IsResponse bool   `protobuf:"varint,5,opt,name=isResponse,proto3" json:"isResponse,omitempty"`
	Addr       string `protobuf:"bytes,6,opt,name=addr,proto3" json:"addr,omitempty"`
}

func (x *InternalMsg) Reset() {
	*x = InternalMsg{}
	if protoimpl.UnsafeEnabled {
		mi := &file_InternalMessage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InternalMsg) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InternalMsg) ProtoMessage() {}

func (x *InternalMsg) ProtoReflect() protoreflect.Message {
	mi := &file_InternalMessage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InternalMsg.ProtoReflect.Descriptor instead.
func (*InternalMsg) Descriptor() ([]byte, []int) {
	return file_InternalMessage_proto_rawDescGZIP(), []int{0}
}

func (x *InternalMsg) GetMessageID() []byte {
	if x != nil {
		return x.MessageID
	}
	return nil
}

func (x *InternalMsg) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *InternalMsg) GetCheckSum() uint64 {
	if x != nil {
		return x.CheckSum
	}
	return 0
}

func (x *InternalMsg) GetInternalID() uint32 {
	if x != nil {
		return x.InternalID
	}
	return 0
}

func (x *InternalMsg) GetIsResponse() bool {
	if x != nil {
		return x.IsResponse
	}
	return false
}

func (x *InternalMsg) GetAddr() net.Addr {
	if x != nil {
		if x.Addr == "" {
			return nil
		}
		udpAddr, err := net.ResolveUDPAddr("udp", x.Addr)
		if err != nil {
			return nil
		}
		var netAddr net.Addr = udpAddr
		return netAddr
	}
	return nil
}

var File_InternalMessage_proto protoreflect.FileDescriptor

var file_InternalMessage_proto_rawDesc = []byte{
	0x0a, 0x15, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x22, 0xb5, 0x01, 0x0a, 0x0b, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x4d, 0x73,
	0x67, 0x12, 0x1c, 0x0a, 0x09, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x49, 0x44, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x49, 0x44, 0x12,
	0x18, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x68, 0x65,
	0x63, 0x6b, 0x53, 0x75, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x06, 0x52, 0x08, 0x63, 0x68, 0x65,
	0x63, 0x6b, 0x53, 0x75, 0x6d, 0x12, 0x1e, 0x0a, 0x0a, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61,
	0x6c, 0x49, 0x44, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0a, 0x69, 0x6e, 0x74, 0x65, 0x72,
	0x6e, 0x61, 0x6c, 0x49, 0x44, 0x12, 0x1e, 0x0a, 0x0a, 0x69, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x69, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x64, 0x64, 0x72, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x61, 0x64, 0x64, 0x72, 0x42, 0x0d, 0x5a, 0x0b, 0x70, 0x62, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_InternalMessage_proto_rawDescOnce sync.Once
	file_InternalMessage_proto_rawDescData = file_InternalMessage_proto_rawDesc
)

func file_InternalMessage_proto_rawDescGZIP() []byte {
	file_InternalMessage_proto_rawDescOnce.Do(func() {
		file_InternalMessage_proto_rawDescData = protoimpl.X.CompressGZIP(file_InternalMessage_proto_rawDescData)
	})
	return file_InternalMessage_proto_rawDescData
}

var file_InternalMessage_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_InternalMessage_proto_goTypes = []interface{}{
	(*InternalMsg)(nil), // 0: protobuf.InternalMsg
}
var file_InternalMessage_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_InternalMessage_proto_init() }
func file_InternalMessage_proto_init() {
	if File_InternalMessage_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_InternalMessage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InternalMsg); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_InternalMessage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_InternalMessage_proto_goTypes,
		DependencyIndexes: file_InternalMessage_proto_depIdxs,
		MessageInfos:      file_InternalMessage_proto_msgTypes,
	}.Build()
	File_InternalMessage_proto = out.File
	file_InternalMessage_proto_rawDesc = nil
	file_InternalMessage_proto_goTypes = nil
	file_InternalMessage_proto_depIdxs = nil
}
