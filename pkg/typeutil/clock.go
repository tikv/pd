package typeutil

import (
	"time"
)

var (
	// RealtimeClock is a settable system-wide clock that measures real (i.e., wall-clock) time.
	// Setting this clock requires appropriate privileges. This clock is affected by
	// discontinuous jumps in the system time (e.g., if the system administrator manually
	// changes the clock), and by the incremental adjustments performed by adjtime(3) and NTP.
	RealtimeClock Clock

	// MonotonicClock is a nonsettable system-wide clock that represents monotonic time since—as described
	// by POSIX—"some unspecified point in the past". On Linux, that point corresponds to the number of seconds
	// that the system has been running since it was booted.
	MonotonicClock Clock

	// MonotonicRawClock is similar to MonotonicClock, but provides access to a raw hardware-based
	// time that is not subject to NTP adjustments or the incremental adjustments performed by adjtime(3).
	// This clock does not count time that the system is suspended.
	MonotonicRawClock Clock
)

// Clock represents a clock which can be queried for a time.Time
type Clock interface {
	Now() time.Time
	Elapsed() time.Duration
}
