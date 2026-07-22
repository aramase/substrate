// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command counter is a simple server that will be used as a worker pod. It listens on ports 80
// and returns a greeting with the IP of the pod where it is running.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
)

var (
	requestCount uint64
	ready        atomic.Bool
	fileMutex    sync.Mutex
	// workingSet is a large, resident, non-zero region allocated at startup and
	// captured in the snapshot. It models a memory-heavy agent's warmed working set
	// (LLM/context/caches). Set WORKING_SET_MIB to enable it; the /work endpoint
	// reads it, which on restore demand-faults it — the cost hugepages reduce.
	workingSet []byte
)

const fileCounterPath = "/home/counter/a.txt"

// workingSetPageSize is the smallest page the guest can fault (4 KiB). touchWorkingSet
// reads one byte per page at this stride so every base page is demand-faulted; 2 MiB
// hugepages, when enabled, still fault in as a unit on the first constituent touch.
const workingSetPageSize = 4096

func incrementFileCounter() int {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	counter := 0
	data, err := os.ReadFile(fileCounterPath)
	if err == nil {
		if i, err := strconv.Atoi(string(data)); err == nil {
			counter = i
		}
	}
	counter++
	err = os.WriteFile(fileCounterPath, []byte(strconv.Itoa(counter)), 0o644)
	if err != nil {
		return -1
	}
	return counter
}

func main() {
	pflag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	initWorkingSet(ctx)

	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fileCounter := incrementFileCounter()

		memoryCounter := atomic.AddUint64(&requestCount, 1)
		currentIP := getCurrentIP()
		response := fmt.Sprintf("hello from: %s | preserved memory count: %d | preserved file counter: %d\n", currentIP, memoryCounter, fileCounter)
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})
	// /readyz is the endpoint the ateom-gvisor readyz probe polls. It returns
	// 200 only once initialization (the random-file write) has completed.
	// After a checkpoint+restore the atomic flag is part of the snapshot, so
	// the endpoint returns 200 immediately on resume.
	defaultMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	// /work reads the entire working set once, forcing every page resident. On a
	// freshly restored guest those pages are un-faulted, so this measures the
	// demand-fault cost of the warmed working set — the time to first useful work.
	// touch_ms in the response is the pure in-guest read time (no network), so a
	// client can record the restore-to-useful-work latency without network noise.
	defaultMux.HandleFunc("/work", func(w http.ResponseWriter, _ *http.Request) {
		start := time.Now()
		sum := touchWorkingSet()
		took := time.Since(start)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "touch_ms=%.3f bytes=%d sum=%d\n", float64(took.Microseconds())/1000.0, len(workingSet), sum)
	})

	go func() {
		slog.InfoContext(ctx, "Starting counter server on port 80")
		if err := http.ListenAndServe(":80", defaultMux); err != nil {
			slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	// Write some random data to a file in the root filesystem, to test
	// filesystem checkpoint/restore.
	if err := writeRandomFile(); err != nil {
		slog.InfoContext(ctx, "Error writing random file", slog.Any("err", err))
	} else {
		slog.InfoContext(ctx, "Wrote content to random file", slog.String("fshash", hashRandomFile()))
	}

	ready.Store(true)
	slog.InfoContext(ctx, "Readyz now reports OK")

	count := 0
	slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
	count++

	for range time.Tick(10 * time.Second) {
		// TODO: Test outbound connectivity by pinging google.com
		slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
		count++
	}
}

func writeRandomFile() error {
	rf, err := os.Create("/random-content-file")
	if err != nil {
		return fmt.Errorf("while opening file: %w", err)
	}
	defer rf.Close()

	_, err = io.CopyN(rf, rand.Reader, 1*1024*1024)
	if err != nil {
		return fmt.Errorf("while copying rand data: %w", err)
	}

	return nil
}

func hashRandomFile() string {
	rfBytes, err := os.ReadFile("/random-content-file")
	if err != nil {
		panic(err)
	}

	hash := sha256.Sum256(rfBytes)
	return base64.RawStdEncoding.EncodeToString(hash[:])
}

func getCurrentIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		slog.Error("Error getting interface addresses", slog.Any("err", err))
		return "x.x.x.x"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "y.y.y.y"
}

// initWorkingSet allocates and fills a WORKING_SET_MIB-sized region with a non-zero
// pattern. Non-zero matters: zero pages are sparsified in the snapshot and would not
// be demand-faulted on restore, so they must carry real data to model a warmed agent
// working set. It is left resident, so the golden snapshot captures it.
func initWorkingSet(ctx context.Context) {
	mib := 0
	if v := os.Getenv("WORKING_SET_MIB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			mib = n
		}
	}
	if mib <= 0 {
		return
	}
	workingSet = make([]byte, mib*1024*1024)
	for i := range workingSet {
		workingSet[i] = byte(i%251 + 1) // never zero
	}
	slog.InfoContext(ctx, "Allocated working set", slog.Int("mib", mib))
}

// touchWorkingSet reads one byte per page across the whole working set, forcing
// every page resident. On a freshly restored guest each page (or each 2 MiB hugepage)
// is demand-faulted here.
func touchWorkingSet() uint64 {
	var sum uint64
	for i := 0; i < len(workingSet); i += workingSetPageSize {
		sum += uint64(workingSet[i])
	}
	return sum
}
