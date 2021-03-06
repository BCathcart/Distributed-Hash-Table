package chainReplication

import (
	"log"
	"net"

	kvstore "github.com/BCathcart/Distributed-Hash-Table/src/kvStore"
	"github.com/BCathcart/Distributed-Hash-Table/src/util"
)

const TRANSFER = 0
const SWEEP = 1

type ReplicationEvent struct {
	code uint8 // 0 = transfer, 1 = sweep
	keys util.KeyRange
	addr *net.Addr
}

var queue = make(chan ReplicationEvent, 20) // TODO: shrink after testing

func putReplicationEvent(code uint8, keys util.KeyRange, addr *net.Addr) {
	queue <- ReplicationEvent{code, keys, addr}
}

func runNextReplicationEvent() {
	event := <-queue
	if event.code == TRANSFER {
		sendDataTransferReq(event.addr, event.keys, false, false)
	} else if event.code == SWEEP {
		kvstore.Sweep(event.keys, func() {
			coarseLock.Lock()
			updateCurrentRange(event.keys.High, true)
			coarseLock.Unlock()
		})
	} else {
		log.Println("ERROR: Unknown ReplicationEvent code")
	}
}
