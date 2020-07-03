package typeutil

import (
	"syscall"
	"time"
	"unsafe"
)

const (
	clockRealtime = iota
	clockMonotonic
	clockMonotonicRaw
)

func init() {
	RealtimeClock = &clock{clockID: clockRealtime}
	MonotonicClock = &clock{clockID: clockMonotonic}
	MonotonicRawClock = &clock{clockID: clockMonotonicRaw}
}

type clock struct {
	clockID uintptr
}

func (c *clock) Now() time.Time {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, c.clockID, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec)
}

var epoch = time.Unix(0, 0)

func (c *clock) Elapsed() time.Duration {
	return c.Now().Sub(epoch)
}
