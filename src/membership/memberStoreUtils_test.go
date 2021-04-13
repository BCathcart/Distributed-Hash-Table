package membership

import (
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"log"
	"testing"
)

var m0 = &pb.Member{Key: 0, Status: STATUS_NORMAL}
var m1 = &pb.Member{Key: 10, Status: STATUS_NORMAL}
var m2 = &pb.Member{Key: 20, Status: STATUS_NORMAL}
var m3 = &pb.Member{Key: 30, Status: STATUS_BOOTSTRAPPING}
var m4 = &pb.Member{Key: 40, Status: STATUS_NORMAL}

func setUpMemberStore() {
	memberStore_ = NewMemberStore()
	memberStore_.members = []*pb.Member{m0, m1, m2, m3, m4}
}

func TestGetSuccessorFromPos(t *testing.T) {
	setUpMemberStore()
	result, pos := getSuccessorFromPos(1)
	if pos != 2 {
		t.Errorf("Error getting successor position: expected %v got %v", pos, 2)
	}
	expected := m2
	if result != expected {
		t.Errorf("Error getting successor: expected %v got %v", expected, result)
	}
}

func TestGetSuccessorFromPosStatus(t *testing.T) {
	setUpMemberStore()
	result, pos := getSuccessorFromPos(2)
	if pos != 4 {
		t.Errorf("Error getting successor position: expected %v got %v", pos, 2)
	}
	expected := m4
	if result != expected {
		t.Errorf("Error getting successor: expected %v got %v", expected, result)
	}
}

func TestGetPredecessor(t *testing.T) {
	setUpMemberStore()
	log.Println(memberStore_.members)
	result, err := getPredecessor(15)
	if err != nil {
		t.Errorf("Error getting predecessor: %v", err)
	}
	expected := m1.Key
	if result != expected {
		t.Errorf("Error getting predecessor: expected %v got %v", expected, result)

	}
}
func TestGetPredecessorWrap(t *testing.T) {
	setUpMemberStore()
	result, err := getPredecessor(5)
	if err != nil {
		t.Errorf("Error getting predecessor: %v", err)
	}
	expected := m0.Key
	if result != expected {
		t.Errorf("Error getting predecessor: expected %v got %v", expected, result)
	}
}
