package main

import (
	"time"
)

// `timeDurationToAttrDuration` converts a time.Duration to the seconds:nanoseconds
// form used in package fission data structures.
func timeDurationToAttrDuration(timeDuration time.Duration) (timeDurationSec uint64, timeDurationNSec uint32) {
	timeDurationSec = uint64(timeDuration / time.Second)
	timeDurationNSec = uint32((timeDuration - (time.Duration(timeDurationSec) * time.Second)).Nanoseconds())

	return
}

// `timeTimeToAttrTime` converts a time.Time to the seconds:nanoseconds
// form used in package fission data structures.
func timeTimeToAttrTime(timeTime time.Time) (timeTimeSec uint64, timeTimeNSec uint32) {
	var (
		unixNano = uint64(timeTime.UnixNano())
	)

	timeTimeSec = unixNano / 1e9
	timeTimeNSec = uint32(unixNano - (timeTimeSec * 1e9))

	return
}
