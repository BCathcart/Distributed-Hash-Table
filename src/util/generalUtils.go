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

func GetIPPort(addrString string) (string, string) {
	return strings.Split(addrString, ":")[0], strings.Split(addrString, ":")[1]
}

func GetAddr(ip string, port int) (*net.Addr, error) {
	addr := ip + ":" + strconv.Itoa(port)
	log.Println("ADDR TO PUT IN UDP ", addr)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	var netAddr net.Addr = udpAddr
	return &netAddr, nil
}

func GetNodeKey(ipStr string, portStr string) uint32 {
	return crc32.ChecksumIEEE([]byte(ipStr + portStr))
}

func Hash(bytes []byte) uint32 {
	return crc32.ChecksumIEEE(bytes)
}
