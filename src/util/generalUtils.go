package util

import (
	"hash/crc32"
	"log"
	"net"
	"strconv"
	"strings"
)

func GetAddressBytes(udpAddr *net.UDPAddr) []byte {
	ipStr := string(udpAddr.IP)
	addrString := CreateAddressString(ipStr, udpAddr.Port)
	return []byte(addrString)
}

func CreateAddressString(ipStr string, port int) string {
	return ipStr + ":" + strconv.Itoa(port)
}

func CreateAddressStringFromAddr(addr *net.Addr) string {
	return CreateAddressString((*addr).(*net.UDPAddr).IP.String(), (*addr).(*net.UDPAddr).Port)
}

func GetIPPort(addrString string) (string, string) {
	return strings.Split(addrString, ":")[0], strings.Split(addrString, ":")[1]
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

func SerializeAddr(addr *net.Addr) []byte {
	ip := (*addr).(*net.UDPAddr).IP.String()
	if ip == "::" {
		ip = "127.0.0.1"
	}

	return []byte(CreateAddressString(ip, (*addr).(*net.UDPAddr).Port))
}

func DeserializeAddr(serAddr []byte) (*net.Addr, error) {
	log.Println(string(serAddr))
	udpAddr, err := net.ResolveUDPAddr("udp", string(serAddr))
	var addr net.Addr = udpAddr
	return &addr, err
}

func GetNodeKey(ipStr string, portStr string) uint32 {
	return crc32.ChecksumIEEE([]byte(ipStr + portStr))
}

func GetAddrKey(addrString *net.Addr) uint32 {
	return GetNodeKey(GetIPPort((*addrString).String()))
}

func Hash(bytes []byte) uint32 {
	return crc32.ChecksumIEEE(bytes)
}

func BetweenKeys(targetKey uint32, lowerKey uint32, upperKey uint32) bool {
	if lowerKey < upperKey {
		return targetKey < upperKey && targetKey > lowerKey
	} else { // Edge case where there's a wrap-around
		return targetKey < lowerKey || targetKey > upperKey
	}
}

// TODO(Brennan): check that this works
func RemoveAddrFromArr(s []*net.Addr, i int) []*net.Addr {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

// Source: Sathish's campuswire post #310
/**
* Returns local IP address
 */
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

//func PrintInternalMsg(iMsg *pb.InternalMsg) {
//	//log.Println("INTERNAL MESSAGE, ID:", iMsg.InternalID)
//	//log.Println(iMsg.MessageID)
//	//log.Println(iMsg.Payload)
//	//log.Println(iMsg.CheckSum)
//	//log.Println(iMsg.IsResponse)
//}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msgID The message ID.
* @param msgPayload The message payload.
* @return The checksum.
 */
func ComputeChecksum(msgID []byte, msgPayload []byte) uint32 {
	return crc32.ChecksumIEEE(append(msgID, msgPayload...))
}
