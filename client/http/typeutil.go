package http

import (
	"fmt"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"

	"strconv"
	"time"
)

// Duration is a wrapper of time.Duration for TOML and JSON.
type Duration struct {
	time.Duration
}

// NewDuration creates a Duration from time.Duration.
func NewDuration(duration time.Duration) Duration {
	return Duration{Duration: duration}
}

// MarshalJSON returns the duration as a JSON string.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.String())), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return errors.WithStack(err)
	}
	d.Duration = duration
	return nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return errors.WithStack(err)
}

// MarshalText returns the duration as a JSON string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// ByteSize is a retype uint64 for TOML and JSON.
type ByteSize uint64

// ParseMBFromText parses MB from text.
func ParseMBFromText(text string, value uint64) uint64 {
	b := ByteSize(0)
	err := b.UnmarshalText([]byte(text))
	if err != nil {
		return value
	}
	return uint64(b / units.MiB)
}

// MarshalJSON returns the size as a JSON string.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(`"` + units.BytesSize(float64(b)) + `"`), nil
}

// UnmarshalJSON parses a JSON string into the byte size.
func (b *ByteSize) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	v, err := units.RAMInBytes(s)
	if err != nil {
		return errors.WithStack(err)
	}
	*b = ByteSize(v)
	return nil
}

// UnmarshalText parses a Toml string into the byte size.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := units.RAMInBytes(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	*b = ByteSize(v)
	return nil
}
