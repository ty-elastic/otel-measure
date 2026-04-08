package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// buildNDJSON constructs a _bulk ndjson body from pairs of (action, document).
// document may be nil for delete actions.
func buildNDJSON(t *testing.T, pairs []bulkPair) (body []byte, docJSONs [][]byte) {
	t.Helper()
	var buf bytes.Buffer
	for _, p := range pairs {
		actionLine, err := json.Marshal(p.action)
		if err != nil {
			t.Fatalf("marshal action: %v", err)
		}
		buf.Write(actionLine)
		buf.WriteByte('\n')

		if p.document != nil {
			docLine, err := json.Marshal(p.document)
			if err != nil {
				t.Fatalf("marshal document: %v", err)
			}
			buf.Write(docLine)
			buf.WriteByte('\n')
			docJSONs = append(docJSONs, docLine)
		}
	}
	return buf.Bytes(), docJSONs
}

// bulkPair holds one action metadata + optional document.
type bulkPair struct {
	action   map[string]any
	document map[string]any // nil for delete
}

// indexAction is a convenience helper for building an index action line.
func indexAction(index, id string) map[string]any {
	return map[string]any{"index": map[string]any{"_index": index, "_id": id}}
}

// deleteAction is a convenience helper for building a delete action line.
func deleteAction(index, id string) map[string]any {
	return map[string]any{"delete": map[string]any{"_index": index, "_id": id}}
}

// expectedMetrics computes the document_bytes and metered_bytes the server
// should report, using the exact bytes that will be sent over the wire.
// It round-trips through JSON unmarshal to match the server's type coercions
// (e.g. all numbers become float64).
func expectedMetrics(t *testing.T, docJSONs [][]byte) (docBytes, meteredBytes int64) {
	t.Helper()
	for _, raw := range docJSONs {
		docBytes += int64(len(raw))
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("unmarshal doc for expected metering: %v", err)
		}
		meteredBytes += meterValue(v)
	}
	return
}

// postBulk sends ndjson body to a test server and decodes the BulkResponse.
func postBulk(t *testing.T, srv *httptest.Server, body []byte) BulkResponse {
	t.Helper()
	resp, err := http.Post(srv.URL+"/_bulk", "application/x-ndjson", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /_bulk: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Errorf("X-Elastic-Product: got %q, want %q", got, "Elasticsearch")
	}

	var br BulkResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return br
}

func TestBulkHandler(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", bulkHandler)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tests := []struct {
		name  string
		pairs []bulkPair
	}{
		{
			name: "flat string fields",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "1"),
					document: map[string]any{"name": "Alice", "city": "London"},
				},
			},
		},
		{
			name: "numeric fields",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "2"),
					document: map[string]any{"count": 42, "ratio": 3.14, "score": -7},
				},
			},
		},
		{
			name: "boolean fields",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "3"),
					document: map[string]any{"active": true, "deleted": false},
				},
			},
		},
		{
			name: "null fields",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "4"),
					document: map[string]any{"name": "Bob", "middle_name": nil},
				},
			},
		},
		{
			name: "nested object",
			pairs: []bulkPair{
				{
					action: indexAction("test", "5"),
					document: map[string]any{
						"user": map[string]any{
							"name": "Carol",
							"age":  30,
							"address": map[string]any{
								"street": "123 Main St",
								"city":   "Springfield",
								"zip":    "12345",
							},
						},
					},
				},
			},
		},
		{
			name: "array of strings",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "6"),
					document: map[string]any{"tags": []any{"go", "elasticsearch", "otel"}},
				},
			},
		},
		{
			name: "array of numbers",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "7"),
					document: map[string]any{"readings": []any{1.1, 2.2, 3.3, 4.4}},
				},
			},
		},
		{
			name: "array of objects",
			pairs: []bulkPair{
				{
					action: indexAction("test", "8"),
					document: map[string]any{
						"events": []any{
							map[string]any{"type": "click", "count": 5},
							map[string]any{"type": "view", "count": 12},
						},
					},
				},
			},
		},
		{
			name: "mixed array",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "9"),
					document: map[string]any{"mixed": []any{"hello", 99, true, nil}},
				},
			},
		},
		{
			name: "base64 binary field",
			pairs: []bulkPair{
				{
					action: indexAction("test", "10"),
					document: map[string]any{
						"name": "Dave",
						"blob": "SGVsbG8gV29ybGQhIFRoaXMgaXMgYmFzZTY0IGVuY29kZWQgYmluYXJ5IGRhdGEu",
					},
				},
			},
		},
		{
			name: "deeply nested structure",
			pairs: []bulkPair{
				{
					action: indexAction("test", "11"),
					document: map[string]any{
						"level1": map[string]any{
							"level2": map[string]any{
								"level3": map[string]any{
									"value":  "deep",
									"number": 100,
									"flag":   true,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple documents in one request",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "12"),
					document: map[string]any{"name": "Eve", "score": 95},
				},
				{
					action:   indexAction("test", "13"),
					document: map[string]any{"name": "Frank", "active": true, "tags": []any{"admin", "user"}},
				},
				{
					action:   indexAction("test", "14"),
					document: map[string]any{"value": 3.14159, "label": "pi"},
				},
			},
		},
		{
			name: "delete action mixed with index",
			pairs: []bulkPair{
				{
					action:   indexAction("test", "15"),
					document: map[string]any{"name": "Grace", "age": 28},
				},
				{
					action:   deleteAction("test", "old-1"),
					document: nil,
				},
				{
					action:   indexAction("test", "16"),
					document: map[string]any{"name": "Heidi", "active": false},
				},
			},
		},
		{
			name: "all value types in one document",
			pairs: []bulkPair{
				{
					action: indexAction("test", "17"),
					document: map[string]any{
						"str":    "hello world",
						"int":    42,
						"float":  2.718,
						"bool":   true,
						"null":   nil,
						"binary": "aGVsbG8=",
						"nested": map[string]any{
							"x": 1,
							"y": 2,
						},
						"arr": []any{"a", 1, false, nil},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, docJSONs := buildNDJSON(t, tt.pairs)
			wantDocBytes, wantMetered := expectedMetrics(t, docJSONs)

			// Count only non-delete pairs as documents
			wantDocCount := int64(len(docJSONs))

			br := postBulk(t, srv, body)

			if br.DocumentCount != wantDocCount {
				t.Errorf("document_count: got %d, want %d", br.DocumentCount, wantDocCount)
			}
			if br.DocumentBytes != wantDocBytes {
				t.Errorf("document_bytes: got %d, want %d", br.DocumentBytes, wantDocBytes)
			}
			if br.MeteredBytes != wantMetered {
				t.Errorf("metered_bytes: got %d, want %d", br.MeteredBytes, wantMetered)
			}
		})
	}
}

// TestBulkHandlerMetering verifies individual field type metering values explicitly.
func TestBulkHandlerMetering(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", bulkHandler)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tests := []struct {
		name        string
		document    map[string]any
		wantMetered int64
	}{
		{
			name:        "single string",
			document:    map[string]any{"k": "hello"}, // "hello" = 5 bytes
			wantMetered: 5,
		},
		{
			name:        "single number",
			document:    map[string]any{"k": 12345}, // always 8 bytes
			wantMetered: 8,
		},
		{
			name:        "single bool true",
			document:    map[string]any{"k": true}, // always 1 byte
			wantMetered: 1,
		},
		{
			name:        "single bool false",
			document:    map[string]any{"k": false}, // always 1 byte
			wantMetered: 1,
		},
		{
			name:        "null value",
			document:    map[string]any{"k": nil}, // 0 bytes
			wantMetered: 0,
		},
		{
			name:        "two strings",
			document:    map[string]any{"a": "hi", "b": "bye"}, // 2 + 3 = 5
			wantMetered: 5,
		},
		{
			name:        "string and number",
			document:    map[string]any{"s": "abc", "n": 1}, // 3 + 8 = 11
			wantMetered: 11,
		},
		{
			name:        "nested object",
			document:    map[string]any{"obj": map[string]any{"x": "ab", "y": 1}}, // 2 + 8 = 10
			wantMetered: 10,
		},
		{
			name:        "array of bools",
			document:    map[string]any{"arr": []any{true, false, true}}, // 1+1+1 = 3
			wantMetered: 3,
		},
		{
			name:        "utf-8 multibyte string",
			document:    map[string]any{"k": "héllo"}, // é is 2 bytes in UTF-8: 1+2+3 = 6
			wantMetered: int64(len("héllo")),
		},
		{
			name:        "base64 binary string",
			document:    map[string]any{"blob": fmt.Sprintf("%s", "SGVsbG8=")}, // 8 chars = 8 bytes
			wantMetered: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs := []bulkPair{{action: indexAction("test", "1"), document: tt.document}}
			body, _ := buildNDJSON(t, pairs)
			br := postBulk(t, srv, body)

			if br.MeteredBytes != tt.wantMetered {
				t.Errorf("metered_bytes: got %d, want %d", br.MeteredBytes, tt.wantMetered)
			}
		})
	}
}
