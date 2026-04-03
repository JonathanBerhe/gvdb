package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Metric represents a single Prometheus metric sample
type Metric struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// GET /api/metrics — fetch Prometheus metrics from GVDB and return as JSON
func (g *Gateway) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	// Derive metrics URL from gRPC address (replace gRPC port with metrics port)
	// GVDB single-node: metrics on port 9090, gRPC on 50051
	// The metrics URL can be overridden via query param
	metricsURL := r.URL.Query().Get("url")
	if metricsURL == "" {
		// Default: assume metrics on same host, port 9090
		host := strings.Split(g.conn.Target(), ":")[0]
		if host == "" {
			host = "localhost"
		}
		metricsURL = fmt.Sprintf("http://%s:9090/metrics", host)
	}

	resp, err := http.Get(metricsURL)
	if err != nil {
		writeError(w, 502, "Failed to fetch metrics: "+err.Error())
		return
	}
	defer resp.Body.Close()

	metrics, err := parsePrometheusText(resp.Body)
	if err != nil {
		writeError(w, 500, "Failed to parse metrics: "+err.Error())
		return
	}

	// Filter to only gvdb_ metrics
	var gvdbMetrics []Metric
	for _, m := range metrics {
		if strings.HasPrefix(m.Name, "gvdb_") {
			gvdbMetrics = append(gvdbMetrics, m)
		}
	}

	writeJSON(w, 200, gvdbMetrics)
}

// parsePrometheusText parses Prometheus exposition format into Metric structs
func parsePrometheusText(reader io.Reader) ([]Metric, error) {
	var metrics []Metric
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		m, err := parseMetricLine(line)
		if err != nil {
			continue // skip malformed lines
		}
		metrics = append(metrics, m)
	}

	return metrics, scanner.Err()
}

// parseMetricLine parses a single Prometheus metric line like:
// gvdb_insert_requests_total{collection="test",status="success"} 42
func parseMetricLine(line string) (Metric, error) {
	m := Metric{Labels: make(map[string]string)}

	// Find value (last space-separated token)
	lastSpace := strings.LastIndex(line, " ")
	if lastSpace < 0 {
		return m, fmt.Errorf("no value found")
	}

	valueStr := strings.TrimSpace(line[lastSpace+1:])
	val, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return m, err
	}
	m.Value = val

	nameAndLabels := strings.TrimSpace(line[:lastSpace])

	// Parse labels if present
	braceStart := strings.Index(nameAndLabels, "{")
	if braceStart >= 0 {
		m.Name = nameAndLabels[:braceStart]
		braceEnd := strings.LastIndex(nameAndLabels, "}")
		if braceEnd > braceStart {
			labelsStr := nameAndLabels[braceStart+1 : braceEnd]
			for _, pair := range splitLabels(labelsStr) {
				eqIdx := strings.Index(pair, "=")
				if eqIdx > 0 {
					key := strings.TrimSpace(pair[:eqIdx])
					val := strings.Trim(strings.TrimSpace(pair[eqIdx+1:]), "\"")
					m.Labels[key] = val
				}
			}
		}
	} else {
		m.Name = nameAndLabels
	}

	return m, nil
}

// splitLabels splits label pairs, respecting quoted values
func splitLabels(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for _, ch := range s {
		switch {
		case ch == '"':
			inQuote = !inQuote
			current.WriteRune(ch)
		case ch == ',' && !inQuote:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}
