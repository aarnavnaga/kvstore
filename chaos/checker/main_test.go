package main

import (
	"math"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func put(c int, k, v string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: c, Input: input{op: "put", key: k, value: v}, Call: call, Output: output{}, Return: ret}
}

func get(c int, k, v string, found bool, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: c, Input: input{op: "get", key: k}, Call: call, Output: output{value: v, found: found}, Return: ret}
}

func check(t *testing.T, ops []porcupine.Operation) porcupine.CheckResult {
	t.Helper()
	return porcupine.CheckOperationsTimeout(model, ops, 10*time.Second)
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	ops := []porcupine.Operation{
		get(0, "k", "", false, 0, 1),
		put(0, "k", "a", 2, 3),
		get(1, "k", "a", true, 4, 5),
	}
	if r := check(t, ops); r != porcupine.Ok {
		t.Fatalf("got %v, want Ok", r)
	}
}

func TestStaleReadIsCaught(t *testing.T) {
	// b is written after a has been acknowledged, and a read that starts after b was
	// acknowledged still returns a: no order of these operations explains that.
	ops := []porcupine.Operation{
		put(0, "k", "a", 0, 1),
		put(0, "k", "b", 2, 3),
		get(1, "k", "a", true, 4, 5),
	}
	if r := check(t, ops); r != porcupine.Illegal {
		t.Fatalf("got %v, want Illegal", r)
	}
}

func TestValueFromNowhereIsCaught(t *testing.T) {
	ops := []porcupine.Operation{
		put(0, "k", "a", 0, 1),
		get(1, "k", "zzz", true, 2, 3),
	}
	if r := check(t, ops); r != porcupine.Illegal {
		t.Fatalf("got %v, want Illegal", r)
	}
}

func TestConcurrentOperationsMayLinearizeEitherWay(t *testing.T) {
	ops := []porcupine.Operation{
		put(0, "k", "a", 0, 10),
		get(1, "k", "", false, 1, 2), // before the put takes effect
		get(2, "k", "a", true, 3, 4), // after
	}
	if r := check(t, ops); r != porcupine.Ok {
		t.Fatalf("got %v, want Ok", r)
	}
}

func TestUnknownWriteMayLandLate(t *testing.T) {
	// A write that timed out (return at infinity) can take effect after later reads.
	ops := []porcupine.Operation{
		put(0, "k", "a", 0, math.MaxInt64),
		get(1, "k", "", false, 5, 6),
		get(1, "k", "a", true, 7, 8),
	}
	if r := check(t, ops); r != porcupine.Ok {
		t.Fatalf("got %v, want Ok", r)
	}
}

func TestResurrectedWriteIsCaught(t *testing.T) {
	// The duplicate-apply bug client sessions prevent: a retried old write lands again
	// after a newer one, so a reader sees the key go back in time.
	ops := []porcupine.Operation{
		put(0, "k", "old", 0, 1),
		put(1, "k", "new", 2, 3),
		get(2, "k", "new", true, 4, 5),
		get(2, "k", "old", true, 6, 7),
	}
	if r := check(t, ops); r != porcupine.Illegal {
		t.Fatalf("got %v, want Illegal", r)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	ops := []porcupine.Operation{
		put(0, "a", "1", 0, 1),
		put(0, "b", "2", 2, 3),
		get(1, "a", "1", true, 4, 5),
		get(1, "b", "2", true, 6, 7),
	}
	if r := check(t, ops); r != porcupine.Ok {
		t.Fatalf("got %v, want Ok", r)
	}
}
