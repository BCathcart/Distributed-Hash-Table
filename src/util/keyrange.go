package util

type KeyRange struct {
	low  uint32
	high uint32
}

func (k KeyRange) IncludesKey(key uint32) bool {
	if k.low < k.high {
		return key <= k.high && key >= k.low
	}
	return key >= k.low || key <= k.high
}
