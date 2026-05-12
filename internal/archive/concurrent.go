package archive

import "runtime"

// defaultSegmentConcurrencyCap bounds the default worker pool used to parallelize
// per-segment fetch/decode/checksum work. The CPU-bound nature of decoding and
// the IO-bound nature of object fetches both benefit from a small pool; capping
// at 8 keeps memory bounded when segments are large (hundreds of MiB each) and
// avoids stampeding the object store.
const defaultSegmentConcurrencyCap = 8

// segmentConcurrency normalizes a requested worker count into a sensible bound.
// A non-positive value selects the default (min(GOMAXPROCS, 8)). The result is
// always at least 1.
func segmentConcurrency(requested int) int {
	if requested > 0 {
		return requested
	}
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	if n > defaultSegmentConcurrencyCap {
		n = defaultSegmentConcurrencyCap
	}
	return n
}
