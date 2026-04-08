package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type Format string

const (
	Table Format = "table"
	JSON  Format = "json"
	CSV   Format = "csv"
)

type TableData struct {
	Headers []string
	Rows    [][]string
}

func Print(format Format, data TableData) {
	switch format {
	case JSON:
		printJSON(os.Stdout, data)
	case CSV:
		printCSV(os.Stdout, data)
	default:
		if isTerminal() {
			printStyledTable(os.Stdout, data)
		} else {
			printPlainTable(os.Stdout, data)
		}
	}
}

func PrintRaw(format Format, v any) {
	switch format {
	case JSON:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(v)
	default:
		fmt.Println(v)
	}
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

var (
	headerStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("8"))
	cellStyle   = lipgloss.NewStyle()
)

func printStyledTable(w io.Writer, data TableData) {
	if len(data.Headers) == 0 {
		return
	}

	widths := make([]int, len(data.Headers))
	for i, h := range data.Headers {
		widths[i] = len(h)
	}
	for _, row := range data.Rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Header
	var hdr strings.Builder
	for i, h := range data.Headers {
		if i > 0 {
			hdr.WriteString("  ")
		}
		hdr.WriteString(fmt.Sprintf("%-*s", widths[i], strings.ToUpper(h)))
	}
	fmt.Fprintln(w, headerStyle.Render(hdr.String()))

	// Rows
	for _, row := range data.Rows {
		var line strings.Builder
		for i, cell := range row {
			if i > 0 {
				line.WriteString("  ")
			}
			if i < len(widths) {
				line.WriteString(fmt.Sprintf("%-*s", widths[i], cell))
			} else {
				line.WriteString(cell)
			}
		}
		fmt.Fprintln(w, cellStyle.Render(line.String()))
	}
}

func printPlainTable(w io.Writer, data TableData) {
	if len(data.Headers) == 0 {
		return
	}
	fmt.Fprintln(w, strings.Join(data.Headers, "\t"))
	for _, row := range data.Rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
}

func printJSON(w io.Writer, data TableData) {
	rows := make([]map[string]string, 0, len(data.Rows))
	for _, row := range data.Rows {
		m := make(map[string]string)
		for i, cell := range row {
			if i < len(data.Headers) {
				m[data.Headers[i]] = cell
			}
		}
		rows = append(rows, m)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(rows)
}

func printCSV(w io.Writer, data TableData) {
	cw := csv.NewWriter(w)
	cw.Write(data.Headers)
	for _, row := range data.Rows {
		cw.Write(row)
	}
	cw.Flush()
}
