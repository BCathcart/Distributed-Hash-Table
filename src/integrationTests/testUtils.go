package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

func intentionallyCrash() {
	log.Fatal("Intentionally Crashing so other tests don't run")
}

func handleKVRequest(status bool, kVRes *pb.KVResponse) {
	if kVRes.ErrCode != 0 {
		fmt.Println("ERROR RESPONSE")
		fmt.Println(kVRes.ErrCode)
		return
	}

	if status == false {
		fmt.Println("ERROR")
		return
	}
}

func GetAddr(ip string, port int) (*net.Addr, error) {
	addr := ip + ":" + strconv.Itoa(port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	var netAddr net.Addr = udpAddr
	return &netAddr, nil
}

func sendUDPRequest(addr *net.Addr, payload []byte) {
	log.Println(ClientConn)
	ip := (*ClientConn).LocalAddr().(*net.UDPAddr).IP.String()
	port := (*ClientConn).LocalAddr().(*net.UDPAddr).Port

	msgID := getmsgID(ip, uint16(port))
	checksum := util.ComputeChecksum(msgID, payload)
	reqMsg := &pb.Msg{
		MessageID: msgID,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}
	kvReq := &pb.KVRequest{}
	err = proto.Unmarshal(payload, kvReq)

	putTestReqCacheEntry(string(reqMsg.MessageID), serMsg, kvReq)

	writeMsg(*addr, serMsg)
}

/**
* Generates a unique 16 byte ID.
* @param clientIP The client's IP address.
* @param port Server port number.
* @return The unique ID as a 16 byte long byte array.
 */
func getmsgID(clientIP string, port uint16) []byte {
	ipArr := strings.Split(clientIP, ".")
	ipBytes := make([]byte, 5)
	for i, s := range ipArr {
		val, _ := strconv.Atoi(s)
		binary.LittleEndian.PutUint16(ipBytes[i:], uint16(val))
	}
	ipBytes = ipBytes[0:4]
	portBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(portBytes, port)
	randBytes := make([]byte, 2)
	rand.Read(randBytes)
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().UnixNano()))
	id := append(append(append(ipBytes, portBytes...), randBytes...), timeBytes...)
	return id
}

func writeMsg(addr net.Addr, msg []byte) {
	_, err := (*ClientConn).WriteTo(msg, addr)
	if err != nil {
		log.Println(err)
	} else {
		// log.Println("INFO:", n, "bytes written.")
	}
}

/**
* Processes a response message.
* @param ClientConn The connection object to send messages over.
* @param addr The IP address to send response to.
* @param serialMsg The incoming message.
* @param handler The message handler callback.
 */
func processResponse(senderAddr net.Addr, resMsg *pb.Msg) {
	log.Println("Processing response")
	// Get cached request (ignore if it's not cached)
	testReqCache_.lock.Lock()
	key := string(resMsg.MessageID)
	req := testReqCache_.data.Get(key)
	if req != nil {
		///************DEBUGGING****************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("Received response", resMsg.MessageID, "for value", kvstore.BytetoInt(res.GetValue()))
		trcKVReq := req.(testReqCacheEntry).kvReq

		putGetCheck(trcKVReq, res)

		//**************************************/
		testReqCache_.data.Delete(key)

	} else {
		log.Println("WARN: Received response for unknown request")
		///************DEBUGGING***************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("WARN: Received response for unknown request with msgID", resMsg.MessageID, "from", senderAddr.String(), "for value", kvstore.BytetoInt(res.GetValue()))
		//***************************************/
	}
	testReqCache_.lock.Unlock()
}

func putGetCheck(req *pb.KVRequest, res *pb.KVResponse) {
	if req.Command == PUT {
		putGetCache_.data[string(req.Key)] = req.Value
		putGetCache_.numPuts++
	} else if req.Command == GET {
		if res.ErrCode == kvstore.NOT_FOUND {
			log.Println("ERR: Couldn't find value for key: " + string(req.Key))
			putGetCache_.failedGets++
		} else if bytes.Compare(putGetCache_.data[string(req.Key)], res.Value) != 0 {
			log.Printf("ERR: PUT ( %v ) != GET ( %v )", putGetCache_.data[string(req.Key)], res.Value)
			putGetCache_.failedGets++
		} else {
			putGetCache_.successfulGets++
		}
	}

}

func intInSlice(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
