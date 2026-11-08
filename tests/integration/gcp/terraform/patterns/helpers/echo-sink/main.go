// Command echo-sink is the local HTTP sink the TF4 composition patterns point
// emulator deliveries at.
//
// A composition proves a delivery chain end to end only when the *far end* is
// observable: a Pub/Sub push subscription or an Eventarc httpEndpoint
// destination must actually reach something. Pointing it at the emulator's own
// API would only prove that an HTTP request left the process; this sink records
// every request (method, path, headers, body) as one JSON line, so a pattern
// driver can assert the payload and CloudEvents headers it received.
//
// Usage (driven by patterns/lib.sh, not directly):
//
//	echo-sink -addr 127.0.0.1:0 -out sink.jsonl -addr-file sink.addr \
//	  -raw-latest sink.body
//
// The listening base URL is written to -addr-file (and printed on stdout) once
// the port is bound, so a caller can start it on an ephemeral port. It answers
// every path and method with 200 and a small JSON body, which is what Pub/Sub
// push and Eventarc treat as a successful delivery.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

// maxBody bounds what one delivery may record; every payload these patterns
// deliver is a small JSON document.
const maxBody = 1 << 20

// record is one received request. Headers keep their full values so a CloudEvent
// header (ce-type, ce-source, ce-subject) can be asserted exactly.
type record struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address (port 0 = ephemeral)")
	out := flag.String("out", "", "JSONL file every request is appended to (required)")
	addrFile := flag.String("addr-file", "", "file the listening base URL is written to")
	rawLatest := flag.String("raw-latest", "", "file overwritten with the most recent raw body")
	flag.Parse()

	if *out == "" {
		fmt.Fprintln(os.Stderr, "echo-sink: -out is required")
		os.Exit(2)
	}
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("echo-sink: open %s: %v", *out, err)
	}
	defer f.Close()

	var mu sync.Mutex
	enc := json.NewEncoder(f)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, maxBody))
		mu.Lock()
		// The raw body is written before the JSONL record: a driver that waits for
		// the record then always finds the matching body.
		if *rawLatest != "" {
			if werr := os.WriteFile(*rawLatest, body, 0o644); werr != nil {
				log.Printf("echo-sink: write %s: %v", *rawLatest, werr)
			}
		}
		err := enc.Encode(record{Method: r.Method, Path: r.URL.Path, Headers: r.Header, Body: string(body)})
		mu.Unlock()
		if err != nil {
			log.Printf("echo-sink: record request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("echo-sink: listen %s: %v", *addr, err)
	}
	base := "http://" + ln.Addr().String()
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(base+"\n"), 0o644); err != nil {
			log.Fatalf("echo-sink: write %s: %v", *addrFile, err)
		}
	}
	fmt.Println(base)
	log.Fatal(http.Serve(ln, mux))
}
