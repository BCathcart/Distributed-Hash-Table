package main

import pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"

const PUT = 0x01
const GET = 0x02
const REMOVE = 0x03
const SHUTDOWN = 0x04
const WIPEOUT = 0x05
const IS_ALIVE = 0x06
const GET_PID = 0x07
const GET_MEMBERSHIP_COUNT = 0x08

func putRequest(key []byte, value []byte, version int32) *pb.KVRequest {
	return &pb.KVRequest{
		Command: PUT,
		Key:     key,
		Value:   value,
		Version: &version,
	}
}

func getRequest(key []byte) *pb.KVRequest {
	return &pb.KVRequest{
		Command: GET,
		Key:     key,
	}
}

func removeRequest(key []byte) *pb.KVRequest {
	return &pb.KVRequest{
		Command: REMOVE,
		Key:     key,
	}
}

func otherRequest(cmd uint32) *pb.KVRequest {
	return &pb.KVRequest{
		Command: cmd,
	}
}
