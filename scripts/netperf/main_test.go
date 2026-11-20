package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"testing"
)

func TestStreamIsDeterministicAndStampsChunks(t *testing.T) {
	n := int64(block + 3*64<<10 + 5)
	a, _ := io.ReadAll(newStream(n, 7))
	b, _ := io.ReadAll(io.LimitReader(newStream(n, 7), n))
	c, _ := io.ReadAll(newStream(n, 8))
	if int64(len(a)) != n || !bytes.Equal(a, b) {
		t.Fatalf("same seed gave %d and %d bytes, equal=%v", len(a), len(b), bytes.Equal(a, b))
	}
	if bytes.Equal(a, c) {
		t.Fatal("different seeds gave the same stream")
	}
	// Chunks repeat the block except for their index, so a duplicated or
	// reordered chunk changes the hash.
	for i := int64(0); i*64<<10 < n-8; i++ {
		if got := binary.LittleEndian.Uint64(a[i*64<<10:]); got != uint64(i) {
			t.Fatalf("chunk %d stamped %d", i, got)
		}
	}
}

func TestPutUploadsEverything(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan int64, 1)
	go func() {
		_ = http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n, _ := io.Copy(io.Discard, r.Body)
			got <- n
		}))
	}()
	const n = 3*block + 17
	if err := put("http://"+ln.Addr().String()+"/sink", n); err != nil {
		t.Fatal(err)
	}
	if k := <-got; k != n {
		t.Fatalf("server received %d bytes, want %d", k, n)
	}
}
