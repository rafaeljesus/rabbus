package circuit

import (
	"errors"
	"testing"
)

var (
	errNetwork = errors.New("Network error")
)

func TestCall(t *testing.T) {
	var threshold int64
	threshold = 0
	attempts := 0
	fn := func() error { return nil }
	b := NewThresholdBreaker(threshold, attempts, 0)
	err := b.Call(fn, 0)
	if err != nil {
		t.Fail()
	}
}

func TestCallWithRetry(t *testing.T) {
	var threshold int64
	threshold = 2
	attempts := 2
	tries := 0
	fn := func() error {
		tries++
		return errNetwork
	}
	b := NewThresholdBreaker(threshold, attempts, 0)
	err := b.Call(fn, 0)
	if err == nil {
		t.Fail()
	}
	if tries != 2 {
		t.Fail()
	}
}
