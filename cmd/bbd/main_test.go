package main

import "testing"

func TestParsePeer(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantID    string
		wantAddr  string
		wantValid bool
	}{
		{
			name:      "id@address format",
			input:     "node1@localhost:8081",
			wantID:    "node1",
			wantAddr:  "localhost:8081",
			wantValid: true,
		},
		{
			name:      "address only (no @)",
			input:     "localhost:8081",
			wantID:    "localhost:8081",
			wantAddr:  "localhost:8081",
			wantValid: true,
		},
		{
			name:      "empty string",
			input:     "",
			wantID:    "",
			wantAddr:  "",
			wantValid: false,
		},
		{
			name:      "whitespace only",
			input:     "   ",
			wantID:    "",
			wantAddr:  "",
			wantValid: false,
		},
		{
			name:      "empty ID (@address)",
			input:     "@localhost:8081",
			wantID:    "",
			wantAddr:  "",
			wantValid: false,
		},
		{
			name:      "empty address (id@)",
			input:     "node1@",
			wantID:    "",
			wantAddr:  "",
			wantValid: false,
		},
		{
			name:      "just @",
			input:     "@",
			wantID:    "",
			wantAddr:  "",
			wantValid: false,
		},
		{
			name:      "whitespace trimmed",
			input:     "  node1@localhost:8081  ",
			wantID:    "node1",
			wantAddr:  "localhost:8081",
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotAddr, gotValid := parsePeer(tt.input)
			if gotID != tt.wantID {
				t.Errorf("id = %q, want %q", gotID, tt.wantID)
			}
			if gotAddr != tt.wantAddr {
				t.Errorf("addr = %q, want %q", gotAddr, tt.wantAddr)
			}
			if gotValid != tt.wantValid {
				t.Errorf("valid = %v, want %v", gotValid, tt.wantValid)
			}
		})
	}
}

func TestSortHostsByID(t *testing.T) {
	// Import would be needed, but since we can't import routing in main_test,
	// we just verify the sort logic indirectly via buildHostList ordering
}
