package main

import "testing"

func TestParseArgsRejectsInvalidStoreRange(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"missing", []string{"pd-add-preparing-stores", "http://pd"}},
		{"bad-start", []string{"pd-add-preparing-stores", "http://pd", "nope", "2"}},
		{"bad-count", []string{"pd-add-preparing-stores", "http://pd", "10", "nope"}},
		{"zero-count", []string{"pd-add-preparing-stores", "http://pd", "10", "0"}},
		{"overflow", []string{"pd-add-preparing-stores", "http://pd", "18446744073709551615", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseArgs(tt.args); err == nil {
				t.Fatalf("parseArgs(%v) returned nil error", tt.args)
			}
		})
	}
}

func TestParseArgsAcceptsValidStoreRange(t *testing.T) {
	opts, err := parseArgs([]string{"pd-add-preparing-stores", "http://pd", "10", "2"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if opts.addr != "http://pd" || opts.startID != 10 || opts.count != 2 {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestDuplicateStoreIDs(t *testing.T) {
	existing := map[uint64]struct{}{10: {}}
	if err := checkDuplicateStoreIDs(existing, 10, 2); err == nil {
		t.Fatal("expected duplicate store ID error")
	}
	if err := checkDuplicateStoreIDs(existing, 20, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
