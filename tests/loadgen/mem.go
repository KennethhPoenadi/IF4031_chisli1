package main

import "runtime"

type runtimeMem struct{ heapMB float64 }

func (m *runtimeMem) read() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.heapMB = float64(ms.Sys) / (1 << 20)
}
