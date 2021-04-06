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

var queue = make(chan ReplicationEvent, 100) // TODO: shrink after testing

func putReplicationEvent(code uint8, keys util.KeyRange, addr *net.Addr) {
	log.Println("\n\n putReplicationEvent() - 1\n\n")
	queue <- ReplicationEvent{code, keys, addr}
	log.Println("\n\n putReplicationEvent() - 2\n\n")
}

func runNextReplicationEvent() {
	log.Println("\n\nrunNextReplicationEvent() - 1\n\n")
	event := <-queue
	log.Println("\n\nrunNextReplicationEvent() - 2 ", event, "\n\n")

	if event.code == TRANSFER {
		sendDataTransferReq(event.addr, event.keys, false, false)
	} else if event.code == SWEEP {
		kvstore.Sweep(event.keys, func() {
			updateCurrentRange(event.keys.High)
		})
	} else {
		log.Fatal("ERROR: Unknown ReplicationEvent code")
	}
}
