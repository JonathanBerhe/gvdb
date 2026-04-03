package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPrintJSON(t *testing.T) {
	var buf bytes.Buffer
	data := TableData{
		Headers: []string{"name", "id"},
		Rows: [][]string{
			{"products", "1"},
			{"docs", "2"},
		},
	}
	printJSON(&buf, data)

	var result []map[string]string
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0]["name"] != "products" {
		t.Errorf("expected products, got %s", result[0]["name"])
	}
}

func TestPrintCSV(t *testing.T) {
	var buf bytes.Buffer
	data := TableData{
		Headers: []string{"name", "id"},
		Rows: [][]string{
			{"products", "1"},
		},
	}
	printCSV(&buf, data)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if lines[0] != "name,id" {
		t.Errorf("expected header 'name,id', got %s", lines[0])
	}
	if lines[1] != "products,1" {
		t.Errorf("expected 'products,1', got %s", lines[1])
	}
}

func TestPrintPlainTable(t *testing.T) {
	var buf bytes.Buffer
	data := TableData{
		Headers: []string{"name", "count"},
		Rows: [][]string{
			{"a", "10"},
			{"b", "20"},
		},
	}
	printPlainTable(&buf, data)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "name") {
		t.Errorf("header missing 'name': %s", lines[0])
	}
}

func TestPrintStyledTable(t *testing.T) {
	var buf bytes.Buffer
	data := TableData{
		Headers: []string{"name", "value"},
		Rows: [][]string{
			{"test", "123"},
		},
	}
	printStyledTable(&buf, data)

	out := buf.String()
	if !strings.Contains(out, "NAME") {
		t.Error("styled table should uppercase headers")
	}
	if !strings.Contains(out, "test") {
		t.Error("styled table should contain row data")
	}
}

func TestEmptyTable(t *testing.T) {
	var buf bytes.Buffer
	data := TableData{Headers: []string{}, Rows: [][]string{}}
	printStyledTable(&buf, data)
	if buf.Len() != 0 {
		t.Error("empty table should produce no output")
	}
}
