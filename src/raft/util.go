package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MinMax(a []int) (min, max int) {
	min, max = a[0], a[0]
	for i := 1; i < len(a); i++ {
		if a[i] < min {
			min = a[i]
		} else if a[i] > max {
			max = a[i]
		}
	}
	return min, max
}
