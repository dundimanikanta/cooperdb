//go:build !unix

package loadgen

// freeBytes cannot tell on this platform, so the free-space check is skipped.
func freeBytes(dir string) (uint64, bool) {
	return 0, false
}
