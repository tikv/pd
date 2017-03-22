package typeutil

import (
	"strconv"
	"strings"

	"github.com/juju/errors"
)

//StringSlice is more friendly to json encode/decode
type StringSlice []string

// MarshalJSON returns the size as a JSON string.
func (s StringSlice) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strings.Join(s, ",") + `"`), nil
}

// UnmarshalJSON parses a JSON string into the bytesize.
func (s *StringSlice) UnmarshalJSON(text []byte) error {
	data, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	*s = strings.Split(data, ",")
	return nil
}
