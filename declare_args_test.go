package rabbus

import (
	"testing"
	"time"
)

func TestDeclareArgs_With(t *testing.T) {
	a := NewDeclareArgs().With("foo", "bar")
	val, found := a.args["foo"]

	if !found {
		t.Fatalf("Key is not found in Declare Args: %q", "foo")
	}

	if val != "bar" {
		t.Fatalf("Key %q value does not match expected: got %q instead of %q", "foo", val, "bar")
	}
}

func TestDeclareArgs_WithMessageTTL(t *testing.T) {
	a := NewDeclareArgs().WithMessageTTL(60 * time.Second)
	val, found := a.args[messageTTL]

	if !found {
		t.Fatalf("Key is not found in Declare Args: %q", messageTTL)
	}

	if val != int64(60000) {
		t.Fatalf("Key %q value does not match expected: got %d instead of %d", messageTTL, val, int64(60000))
	}
}
