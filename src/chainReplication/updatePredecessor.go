package chainReplication

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

func UpdatePredecessors(addr []*net.Addr, keys []uint32) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	var newPredecessors [3]*predecessorNode
	for i := 0; i < 3; i++ {
		if addr[i] != nil {
			newPredecessors[i] = &predecessorNode{}
			newPredecessors[i].addr = addr[i]
			newPredecessors[i].keys.High = keys[i]
			if i < 2 && addr[i+1] != nil {
				newPredecessors[i].keys.Low = keys[i+1] + 1
			} else {
				newPredecessors[i].keys.Low = MyKeys.High + 1
			}
		} else {
			newPredecessors[i] = nil
			break
		}
	}

	if MyKeys.Low == 0 && MyKeys.High == 0 {
		log.Println("WARN: Keys have not been set yet")
		return
	}

	if newPredecessors[1] != nil {
		// log.Println(1)
		updateRange(util.KeyRange{(*newPredecessors[1]).keys.Low, MyKeys.High}, (*newPredecessors[0]).addr)
	} else if newPredecessors[0] != nil {
		// log.Println(2)
		updateRange(util.KeyRange{(*newPredecessors[0]).keys.Low, MyKeys.High}, (*newPredecessors[0]).addr)
	} else {
		// log.Println(3)
		updateRange(util.KeyRange{MyKeys.High + 1, MyKeys.High}, nil)
	}

	copyPredecessors(newPredecessors)
	if newPredecessors[0] != nil {
		MyKeys.Low = newPredecessors[0].keys.High + 1
	} else {
		MyKeys.Low = MyKeys.High + 1
	}
}

/**
Helper function to copy all new nodes to old nodes by value.
*/
func copyPredecessors(newPredecessors [3]*predecessorNode) {
	for i := 0; i < len(newPredecessors); i++ {
		predecessors[i] = shallowCopy(newPredecessors[i])
	}
}

func updateRange(newRange util.KeyRange, predAddr *net.Addr) {
	printKeyState(&newRange)

	if responsibleRange.Low == newRange.Low && responsibleRange.High == newRange.High {
		return // no updates
	} else if util.BetweenKeys(newRange.Low, responsibleRange.Low, responsibleRange.High) {
		// Key space has shrunk - delete everything outside the new range
		log.Println("UPDATING RANGE - SWEEP")
		putReplicationEvent(SWEEP, util.KeyRange{newRange.High, newRange.Low}, nil)
	} else if util.BetweenKeys(newRange.Low, responsibleRange.High, responsibleRange.Low-1) {
		// key space has grown
		log.Println("UPDATING RANGE - TRANSFER for ", util.KeyRange{newRange.Low, responsibleRange.Low - 1})
		putReplicationEvent(TRANSFER, util.KeyRange{newRange.Low, responsibleRange.Low - 1}, predAddr)
	}

	responsibleRange.Low = newRange.Low
}

/*
	Helper function to copy a predecessor node by value.
*/
func shallowCopy(orig *predecessorNode) *predecessorNode {
	if orig == nil {
		return nil
	}
	return &predecessorNode{
		keys: orig.keys,
		addr: orig.addr,
	}
}
