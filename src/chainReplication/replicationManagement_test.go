package chainReplication

import (
	"github.com/BCathcart/Distributed-Hash-Table/src/util"
	"log"
	"testing"
)

func krArrEqual(arr1 []util.KeyRange, arr2 []util.KeyRange) bool {
	if len(arr1) != len(arr2) {
		log.Println("Lengths unequal")
		return false
	}
	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			log.Println(arr1[i])
			log.Println(arr2[i])
			return false
		}
	}
	return true
}

func TestRemoveKeyRangeFromArr(t *testing.T) {
	kr1 := util.KeyRange{0, 4}
	kr2 := util.KeyRange{5, 8}
	kr3 := util.KeyRange{9, 12}
	s := []util.KeyRange{kr1, kr2, kr3}
	result := removeKeyRangeFromArr(s, 1)
	expected := []util.KeyRange{kr1, kr3}
	if !krArrEqual(result, expected) {
		t.Errorf("Error removing key range from arr")
	}
}

func TestRemoveSendingTransfer(t *testing.T) {
	kr1 := util.KeyRange{0, 4}
	kr2 := util.KeyRange{5, 8}
	kr3 := util.KeyRange{9, 12}
	sendingTransfers = []util.KeyRange{kr1, kr2, kr3}
	expected := []util.KeyRange{kr1, kr2}
	result := removeSendingTransfer(kr3)
	if result == false || !krArrEqual(sendingTransfers, expected) {
		t.Errorf("Error removing key range from arr")
	}
}
