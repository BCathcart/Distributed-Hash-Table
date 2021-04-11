package chainReplication

import (
	"log"
	"net"

	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
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
		log.Fatal("ERROR: Unknown ReplicationEvent code")
	}
}
