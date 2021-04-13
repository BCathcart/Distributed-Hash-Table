package util

import (
	"testing"
)

func TestBTKeys(t *testing.T) {
	targetKey := uint32(5)
	lowerKey := uint32(0)
	upperKey := uint32(10)
	result := BetweenKeys(targetKey, lowerKey, upperKey)
	expected := true
	if result != expected {
		t.Errorf("Error testing base implementation of between keys, expected %v but got %v", expected, result)
	}
}

func TestBTKeysWrapAround(t *testing.T) {
	targetKey := uint32(5)
	lowerKey := uint32(99)
	upperKey := uint32(10)
	result := BetweenKeys(targetKey, lowerKey, upperKey)
	expected := true
	if result != expected {
		t.Errorf("Error testing wrap around for between keys, expected %v but got %v", expected, result)
	}
}

func TestBTKeysFail(t *testing.T) {
	targetKey := uint32(0)
	lowerKey := uint32(1)
	upperKey := uint32(10)
	result := BetweenKeys(targetKey, lowerKey, upperKey)
	expected := false
	if result != expected {
		t.Errorf("Error expected %v but got %v", expected, result)
	}
}

func TestOverlappingKeyRange(t *testing.T) {
	kr1 := KeyRange{0, 5}
	kr2 := KeyRange{4, 10}
	result := OverlappingKeyRange(kr1, kr2)
	expected := &KeyRange{4, 5}
	if result != expected {
		t.Errorf("Error expected %v but got %v", expected, result)
	}
}
func TestOverlappingKeyRangeFail(t *testing.T) {
	kr1 := KeyRange{0, 5}
	kr2 := KeyRange{7, 10}
	result := OverlappingKeyRange(kr1, kr2)
	if result != nil {
		t.Errorf("Error expected nil but got %v", result)
	}
}

func TestGetIPPort(t *testing.T) {
	addr := "111.1.1.1:8080"
	resIP, resPort := GetIPPort(addr)
	expectedIP := "111.1.1.1"
	expectedPort := "8080"
	if resIP != expectedIP {
		t.Errorf("Error: returned wrong IP, expected %v got %v", expectedIP, resIP)
	}
	if resPort != expectedPort {
		t.Errorf("Error: returned wrong Port, expected %v got %v", expectedPort, resPort)
	}

}
