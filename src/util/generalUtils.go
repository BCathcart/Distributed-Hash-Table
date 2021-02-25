package util

import (
	"hash/crc32"
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

func GetIPPort(addrString string) (string, string) {
	return strings.Split(addrString, ":")[0], strings.Split(addrString, ":")[1]
}

func GetNodeKey(ipStr string, portStr string) uint32 {
	return crc32.ChecksumIEEE([]byte(ipStr + portStr))
}
