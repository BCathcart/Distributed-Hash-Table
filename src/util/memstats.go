package util

import (
	"log"
	"runtime"
)

/**
* Prints the process' memory statistics.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
const DONTPRINT = 1

func PrintMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if DONTPRINT == 0 {
		log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		log.Printf("\t Stack = %v\n", bToMb(m.StackSys))
		log.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		log.Printf("\tSys = %v MiB", bToMb(m.Sys))
		log.Printf("\tNum GC cycles = %v\n", m.NumGC)
	}
}

/**
* Converts bytes to megabytes.
* @param b The byte amount.
* @return The corresponding MB amount.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
