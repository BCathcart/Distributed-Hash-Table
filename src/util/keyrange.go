package util

type KeyRange struct {
	Low  uint32
	High uint32
}

func (k KeyRange) IncludesKey(key uint32) bool {
	if k.Low < k.High {
		return key <= k.High && key >= k.Low
	}
	return key >= k.Low || key <= k.High
}
