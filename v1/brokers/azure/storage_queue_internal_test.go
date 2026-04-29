package azure

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBadRequestErrRegex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		match bool
	}{
		{"400", true},
		{"401", true},
		{"403", true},
		{"404", true},
		{"499", true},
		{"200", false},
		{"301", false},
		{"500", false},
		{"503", false},
		{"some 400 error", true},
		{"some 500 error", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.match, badRequestErrRegex.MatchString(tc.input))
		})
	}
}
