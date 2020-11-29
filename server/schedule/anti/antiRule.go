package anti

// AntiRule is the anti affinity rule that can be checked against a region.
type AntiRule struct {
	ID          uint64 `json:"id"`        // unique ID within a group
	StartKey    []byte `json:"-"`         // range start key
	StartKeyHex string `json:"start_key"` // hex format start key, for marshal/unmarshal
	EndKey      []byte `json:"-"`         // range end key
	EndKeyHex   string `json:"end_key"`   // hex format end key, for marshal/unmarshal
}
