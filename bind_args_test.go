package rabbus

import "testing"

func TestBindArgs_With(t *testing.T) {
	a := NewBindArgs().With("foo", "bar")
	val, found := a.args["foo"]

	if !found {
		t.Fatalf("Key is not found in Bind Args: %q", "foo")
	}

	if val != "bar" {
		t.Fatalf("Key %q value does not match expected: got %q instead of %q", "foo", val, "bar")
	}
}
