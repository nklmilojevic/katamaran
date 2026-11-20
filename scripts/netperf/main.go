// netperf: the traffic source and sink for scripts/netperf.sh. One binary runs
// on the Mac (serving 127.0.0.1, reached from containers as
// host.docker.internal) and inside the container (behind a published port).
//
//	GET  /bytes?n=N[&seed=S]  N bytes; with a seed, a deterministic stream
//	GET  /sha?n=N&seed=S      sha256 of that stream, without sending it
//	PUT  /sink                read and discard the body; replies "bytes seconds"
//	GET  /small               a two-byte body, for request/response latency
//
// With -lat URL it times -n sequential GETs and prints "mean p50 p99 max" in
// microseconds (-newconn: one TCP connection per request).
// With -put URL it is instead a client that uploads -n zero bytes to URL and
// prints the rate in bytes per second (curl's upload path is the bottleneck
// above about 2 GB/s).
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"time"
)

const block = 4 << 20

// stream yields n bytes: a 4 MiB pseudo-random block from the seed, repeated,
// with each 64 KiB chunk stamped with its index so reordered, lost or
// duplicated chunks change the hash.
type stream struct {
	base []byte
	buf  []byte
	off  int64
	n    int64
}

func newStream(n int64, seed uint64) *stream {
	base := make([]byte, block)
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	for i := 0; i < block; i += 8 {
		binary.LittleEndian.PutUint64(base[i:], r.Uint64())
	}
	return &stream{base: base, buf: make([]byte, block), n: n}
}

func (s *stream) Read(p []byte) (int, error) {
	if s.off >= s.n {
		return 0, io.EOF
	}
	// Fill a whole block at a time, then copy out of it.
	pos := s.off % block
	if pos == 0 {
		copy(s.buf, s.base)
		for c := int64(0); c < block; c += 64 << 10 {
			binary.LittleEndian.PutUint64(s.buf[c:], uint64((s.off+c)>>16))
		}
	}
	end := min(int64(block), pos+int64(len(p)), pos+s.n-s.off)
	k := copy(p, s.buf[pos:end])
	s.off += int64(k)
	return k, nil
}

func parseN(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address")
	portFile := flag.String("port-file", "", "write the bound port here")
	putURL := flag.String("put", "", "client mode: upload -n bytes to this URL")
	putN := flag.Int64("n", 1<<30, "bytes to upload in client mode")
	latURL := flag.String("lat", "", "client mode: time -n GET requests to this URL")
	newConn := flag.Bool("newconn", false, "with -lat: a new TCP connection per request")
	flag.Parse()
	if *latURL != "" {
		if err := latency(*latURL, int(*putN), *newConn); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *putURL != "" {
		if err := put(*putURL, *putN); err != nil {
			log.Fatal(err)
		}
		return
	}

	zeros := make([]byte, block)
	mux := http.NewServeMux()
	mux.HandleFunc("/small", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/bytes", func(w http.ResponseWriter, r *http.Request) {
		n, err := parseN(r)
		if err != nil || n < 0 {
			http.Error(w, "bad n", 400)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		if s := r.URL.Query().Get("seed"); s != "" {
			seed, _ := strconv.ParseUint(s, 10, 64)
			_, _ = io.CopyBuffer(w, newStream(n, seed), make([]byte, block))
			return
		}
		for n > 0 {
			k := min(n, int64(len(zeros)))
			if _, err := w.Write(zeros[:k]); err != nil {
				return
			}
			n -= k
		}
	})
	mux.HandleFunc("/sha", func(w http.ResponseWriter, r *http.Request) {
		n, err := parseN(r)
		seed, err2 := strconv.ParseUint(r.URL.Query().Get("seed"), 10, 64)
		if err != nil || err2 != nil {
			http.Error(w, "bad n or seed", 400)
			return
		}
		h := sha256.New()
		_, _ = io.CopyBuffer(h, newStream(n, seed), make([]byte, block))
		fmt.Fprintln(w, hex.EncodeToString(h.Sum(nil)))
	})
	mux.HandleFunc("/sink", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		n, err := io.CopyBuffer(io.Discard, r.Body, make([]byte, block))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "%d %.6f\n", n, time.Since(start).Seconds())
	})

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	if *portFile != "" {
		_, p, _ := net.SplitHostPort(ln.Addr().String())
		if err := os.WriteFile(*portFile, []byte(p+"\n"), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	log.Fatal(http.Serve(ln, mux))
}

// put sends the request by hand so the body goes out in 4 MiB writes straight
// to the socket; net/http's client path tops out near 3.5 GB/s on loopback.
func put(rawURL string, n int64) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return err
	}
	defer conn.Close()
	start := time.Now()
	fmt.Fprintf(conn, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		u.RequestURI(), u.Host, n)
	buf := make([]byte, block)
	for left := n; left > 0; {
		k := min(left, int64(len(buf)))
		if _, err := conn.Write(buf[:k]); err != nil {
			return err
		}
		left -= k
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", resp.Status, body)
	}
	fmt.Printf("%.0f\n", float64(n)/time.Since(start).Seconds())
	return nil
}

func latency(url string, n int, newConn bool) error {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = newConn
	client := &http.Client{Transport: tr}
	get := func() error {
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return err
	}
	// Warm up: the first request pays for DNS and, with keep-alive, the dial.
	for range 20 {
		if err := get(); err != nil {
			return err
		}
	}
	d := make([]time.Duration, n)
	var sum time.Duration
	for i := range d {
		t := time.Now()
		if err := get(); err != nil {
			return err
		}
		d[i] = time.Since(t)
		sum += d[i]
	}
	slices.Sort(d)
	us := func(x time.Duration) float64 { return float64(x.Nanoseconds()) / 1e3 }
	fmt.Printf("%.1f %.1f %.1f %.1f\n", us(sum)/float64(n), us(d[n/2]), us(d[n*99/100]), us(d[n-1]))
	return nil
}
