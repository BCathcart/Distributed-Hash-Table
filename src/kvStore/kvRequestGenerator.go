package kvstore

import (
	"errors"
	"fmt"
	"strconv"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
	"google.golang.org/protobuf/proto"
)

func GetPutRequest(key []byte) ([]byte, error) {
	value, ver, isExist := kvStore_.Get(string(key)) //TODO fix type
	if isExist == NOT_FOUND {
		// TODO: return an error? - key not found in KVStore
		return nil, errors.New("KEY NOT FOUND")
	}
	putReq := putRequest(key, value, ver)
	payload, err := serializeReqPayload(putReq)
	if err != nil {
		// assuming an entry that failed to be serialized will be retried once the rest are sent
		return nil, err
	}
	return payload, nil
}

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

/**
* Basic serialization of a request payload object.
* Required for asynchronous requests
* @param structPayload The payload object as a struct
* @return serReqPayload serialized request payload
* @return err An error flag indicates if marshaling failed
 */
func serializeReqPayload(structPayload *pb.KVRequest) ([]byte, error) {
	serReqPayload, err := proto.Marshal(structPayload)
	if err != nil {
		fmt.Println("Marshaling payload error. ", err.Error())
		return nil, err
	}
	return serReqPayload, err
}

/*GetKeyList returns a list of all keys
 * Interface for getting a list of all keys in kvStore from outside kvStore layer
 * @return keyList A []int with all the keys stored in this kvStore
 */
func GetKeyList() []int {
	return kvStore_.getAllKeys()
}

/*RemoveKey removes the entry with given key from local kvStore
 * Interface for calling Remove() on local kvStore
 * @param key The key to be removed from the kvStore
 * @return a status indicating whether entry was successfully removed or not found
 */
func RemoveKey(key int) uint32 {
	keyStr := strconv.Itoa(key)
	return kvStore_.Remove(keyStr)
}
