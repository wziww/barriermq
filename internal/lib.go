package internal

// bit caculate
func GetSize(s uint64) uint64 {
	var r uint64 = 1 << 1
	for r < s {
		r <<= 1
	}
	return r
}
