package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	requestCount          atomic.Int64
	bytesReceived         atomic.Int64
	documentCount         atomic.Int64
	documentBytesReceived atomic.Int64
	totalMeteredBytes     atomic.Int64
)

// ActionParams holds the metadata fields common to all bulk action types.
type ActionParams struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

// ActionMeta is the first line of each bulk pair — exactly one field will be set.
type ActionMeta struct {
	Index  *ActionParams `json:"index"`
	Create *ActionParams `json:"create"`
	Update *ActionParams `json:"update"`
	Delete *ActionParams `json:"delete"`
}

// BulkItem is a decoded action+document pair. Document is nil for delete actions.
type BulkItem struct {
	Meta     ActionMeta
	Document json.RawMessage
}

// meterValue recursively walks a decoded JSON value and returns its metered byte count:
//   - string: UTF-8 byte length (base64 binary fields are strings, so this covers both)
//   - number: 8 bytes
//   - bool:   1 byte
//   - object/array: sum of all nested values
//   - null:   0 bytes
func meterValue(v any) int64 {
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return 8
	case bool:
		return 1
	case map[string]any:
		var total int64
		for _, child := range val {
			total += meterValue(child)
		}
		return total
	case []any:
		var total int64
		for _, elem := range val {
			total += meterValue(elem)
		}
		return total
	case nil: // JSON null
		return 0
	default:
		log.Printf("meterValue: unknown type %T: %v", v, v)
		return 0
	}
}

func parseBulk(body []byte) ([]BulkItem, error) {
	var items []BulkItem

	scanner := bufio.NewScanner(bytes.NewReader(body))
	// Allow lines up to 10 MB.
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		metaLine := scanner.Bytes()

		var meta ActionMeta
		if err := json.Unmarshal(metaLine, &meta); err != nil {
			return nil, err
		}

		item := BulkItem{Meta: meta}

		// Delete actions have no document line.
		if meta.Delete != nil {
			items = append(items, item)
			continue
		}

		if !scanner.Scan() {
			break
		}
		docLine := scanner.Bytes()

		doc := make(json.RawMessage, len(docLine))
		copy(doc, docLine)

		// Validate it's well-formed JSON.
		if !json.Valid(doc) {
			return nil, &json.SyntaxError{}
		}

		item.Document = doc
		items = append(items, item)
	}

	return items, scanner.Err()
}

func bulkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyReader := io.Reader(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			log.Printf("error creating gzip reader: %v", err)
			http.Error(w, "error decompressing body", http.StatusBadRequest)
			return
		}
		defer gr.Close()
		bodyReader = gr
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		log.Printf("error reading bulk body: %v", err)
		http.Error(w, "error reading body", http.StatusInternalServerError)
		return
	}

	items, err := parseBulk(body)
	if err != nil {
		log.Printf("error parsing bulk body: %v\nbody:\n%s", err, body)
		http.Error(w, "error parsing bulk body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var metered, docBytes int64
	for _, item := range items {
		if item.Document == nil {
			continue
		}
		docBytes += int64(len(item.Document))
		var doc any
		if err := json.Unmarshal(item.Document, &doc); err != nil {
			log.Printf("error unmarshaling document for metering: %v", err)
			continue
		}
		metered += meterValue(doc)
	}

	requestCount.Add(1)
	bytesReceived.Add(int64(len(body)))
	documentCount.Add(int64(len(items)))
	documentBytesReceived.Add(docBytes)
	totalMeteredBytes.Add(metered)

	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"errors":false,"items":[]}`))
}

func statsLogger(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()

	for range ticker.C {
		elapsed := time.Since(start).Seconds()
		count := requestCount.Load()
		bytes := bytesReceived.Load()
		docs := documentCount.Load()
		docBytes := documentBytesReceived.Load()
		metered := totalMeteredBytes.Load()

		rps := float64(count) / elapsed
		throughput := float64(bytes) / elapsed / 1024 / 1024
		dps := float64(docs) / elapsed
		totalGBPerHour := float64(bytes) / elapsed * 3600 / 1024 / 1024 / 1024
		docGBPerHour := float64(docBytes) / elapsed * 3600 / 1024 / 1024 / 1024
		meteredGBPerHour := float64(metered) / elapsed * 3600 / 1024 / 1024 / 1024

		log.Printf("requests/s=%.0f  docs/s=%.0f  throughput=%.2f MB/s  total_gigabytes/hour=%.4f  document_gigabytes/hour=%.4f  metered_gigabytes/hour=%.4f  total_requests=%d  total_docs=%d  total_bytes=%d  total_metered_bytes=%d",
			rps, dps, throughput, totalGBPerHour, docGBPerHour, meteredGBPerHour, count, docs, bytes, metered)
	}
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	statsInterval := flag.Duration("stats", 10*time.Second, "stats logging interval")
	readTimeout := flag.Duration("read-timeout", 5*time.Second, "HTTP read timeout")
	writeTimeout := flag.Duration("write-timeout", 5*time.Second, "HTTP write timeout")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", bulkHandler)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadTimeout:       *readTimeout,
		WriteTimeout:      *writeTimeout,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go statsLogger(*statsInterval)

	go func() {
		log.Printf("otel-measurement listening on %s", ln.Addr())
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	if err := srv.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
