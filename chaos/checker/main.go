// checker verifies that a history recorded by kvhistory is linearizable, using
// Porcupine (github.com/anishathalye/porcupine) with a per-key register model.
//
//	checker -history out/leader-crash-1.jsonl [-viz out/leader-crash-1.html]
//
// Exit status 0: linearizable. 1: a violation (with a visualization written if -viz is
// set). 2: the check timed out or the input was malformed.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

type record struct {
	Client int    `json:"client"`
	Op     string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Call   int64  `json:"call"`
	Ret    int64  `json:"ret"`
	Status string `json:"status"`
}

type input struct {
	op    string // "get", "put", "delete"
	key   string
	value string // put only
}

type output struct {
	value string
	found bool // get only
}

// state is one key's register: absent, or holding a value. Every value kvhistory writes
// is unique and non-empty, so "" can stand for absent.
type state = string

var model = porcupine.Model{
	// Keys are independent registers, so each key's sub-history is checked on its own.
	// This is what keeps checking tractable: the search is exponential in concurrency
	// per partition, not across the whole history.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(input).key
			if _, ok := byKey[k]; !ok {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return state("") },
	Step: func(s, in, out interface{}) (bool, interface{}) {
		st := s.(state)
		i := in.(input)
		o := out.(output)
		switch i.op {
		case "get":
			if !o.found {
				return st == "", st
			}
			return st == o.value, st
		case "put":
			return true, state(i.value)
		case "delete":
			return true, state("")
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool { return a.(state) == b.(state) },
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		o := out.(output)
		switch i.op {
		case "get":
			if !o.found {
				return fmt.Sprintf("get(%s) -> not found", i.key)
			}
			return fmt.Sprintf("get(%s) -> %s", i.key, o.value)
		case "put":
			return fmt.Sprintf("put(%s, %s)", i.key, i.value)
		default:
			return fmt.Sprintf("delete(%s)", i.key)
		}
	},
	DescribeState: func(s interface{}) string {
		if s.(state) == "" {
			return "<absent>"
		}
		return s.(state)
	},
}

func load(path string) ([]porcupine.Operation, map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	counts := map[string]int{}
	var ops []porcupine.Operation
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", len(ops)+1, err)
		}
		counts[r.Status]++
		op := porcupine.Operation{
			ClientId: r.Client,
			Input:    input{op: r.Op, key: r.Key, value: r.Value},
			Call:     r.Call,
			Return:   r.Ret,
		}
		switch {
		case r.Op == "get":
			op.Output = output{value: r.Value, found: r.Status == "ok"}
		case r.Status == "unknown":
			// The write may have been applied at any moment after its call, or never;
			// returning at infinity lets the checker place it anywhere from its call on.
			op.Output = output{}
			op.Return = math.MaxInt64
		default:
			op.Output = output{}
		}
		ops = append(ops, op)
	}
	return ops, counts, sc.Err()
}

func main() {
	history := flag.String("history", "", "kvhistory output (JSON lines)")
	viz := flag.String("viz", "", "write an HTML visualization of a violation here")
	timeout := flag.Duration("timeout", 5*time.Minute, "give up after this long")
	flag.Parse()
	if *history == "" {
		fmt.Fprintln(os.Stderr, "usage: checker -history file.jsonl [-viz out.html]")
		os.Exit(2)
	}
	ops, counts, err := load(*history)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(2)
	}
	start := time.Now()
	result, info := porcupine.CheckOperationsVerbose(model, ops, *timeout)
	elapsed := time.Since(start).Round(time.Millisecond)
	fmt.Printf("ops=%d ok=%d notfound=%d unknown=%d result=%s check_time=%s\n",
		len(ops), counts["ok"], counts["notfound"], counts["unknown"], result, elapsed)
	switch result {
	case porcupine.Ok:
		os.Exit(0)
	case porcupine.Illegal:
		if *viz != "" {
			if err := porcupine.VisualizePath(model, info, *viz); err != nil {
				fmt.Fprintln(os.Stderr, "visualize:", err)
			}
		}
		os.Exit(1)
	default:
		os.Exit(2)
	}
}
