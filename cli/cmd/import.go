package cmd

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	importCollection string
	importFile       string
	importFormat     string
	importBatchSize  int
	importIDField    string
	importVecField   string
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Bulk import vectors from file",
	Long:  "Import vectors from JSONL, JSON, or CSV files using streaming inserts.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if importCollection == "" {
			return fmt.Errorf("--collection is required")
		}
		if importFile == "" {
			return fmt.Errorf("--file is required")
		}

		format := importFormat
		if format == "" {
			format = detectFormat(importFile)
		}

		f, err := os.Open(importFile)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()

		var vectors []*pb.VectorWithId
		switch format {
		case "jsonl":
			vectors, err = readJSONL(f)
		case "json":
			vectors, err = readJSON(f)
		case "csv":
			vectors, err = readCSV(f)
		default:
			return fmt.Errorf("unsupported format %q (jsonl, json, csv)", format)
		}
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		if len(vectors) == 0 {
			fmt.Println("No vectors found in file")
			return nil
		}

		// Stream insert in batches
		stream, err := cli.Service.StreamInsert(cli.Stream())
		if err != nil {
			return fmt.Errorf("failed to start stream: %s", client.FormatError(err))
		}

		total := 0
		for i := 0; i < len(vectors); i += importBatchSize {
			end := i + importBatchSize
			if end > len(vectors) {
				end = len(vectors)
			}
			batch := vectors[i:end]

			if err := stream.Send(&pb.InsertRequest{
				CollectionName: importCollection,
				Vectors:        batch,
			}); err != nil {
				return fmt.Errorf("stream send failed at batch %d: %s", i/importBatchSize, client.FormatError(err))
			}
			total += len(batch)
			fmt.Printf("\rSent %d/%d vectors...", total, len(vectors))
		}

		resp, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("stream close failed: %s", client.FormatError(err))
		}

		fmt.Println()
		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]any{
				"inserted_count": resp.InsertedCount,
				"message":        resp.Message,
			})
			return nil
		}

		fmt.Printf("Imported %d vectors into %q\n", resp.InsertedCount, importCollection)
		return nil
	},
}

func detectFormat(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".jsonl", ".ndjson":
		return "jsonl"
	case ".json":
		return "json"
	case ".csv":
		return "csv"
	default:
		return "jsonl"
	}
}

type vectorRecord struct {
	ID       uint64         `json:"id"`
	Values   []float32      `json:"values"`
	Vector   []float32      `json:"vector"`
	Metadata map[string]any `json:"metadata"`
}

func recordToProto(r vectorRecord) *pb.VectorWithId {
	values := r.Values
	if len(values) == 0 {
		values = r.Vector
	}
	v := &pb.VectorWithId{
		Id: r.ID,
		Vector: &pb.Vector{
			Values:    values,
			Dimension: uint32(len(values)),
		},
	}
	if len(r.Metadata) > 0 {
		v.Metadata = mapToMetadata(r.Metadata)
	}
	return v
}

func readJSONL(r io.Reader) ([]*pb.VectorWithId, error) {
	var vectors []*pb.VectorWithId
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0), 10*1024*1024) // 10MB line buffer
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var rec vectorRecord
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		vectors = append(vectors, recordToProto(rec))
	}
	return vectors, scanner.Err()
}

func readJSON(r io.Reader) ([]*pb.VectorWithId, error) {
	var records []vectorRecord
	if err := json.NewDecoder(r).Decode(&records); err != nil {
		return nil, err
	}
	vectors := make([]*pb.VectorWithId, 0, len(records))
	for _, rec := range records {
		vectors = append(vectors, recordToProto(rec))
	}
	return vectors, nil
}

func readCSV(r io.Reader) ([]*pb.VectorWithId, error) {
	reader := csv.NewReader(r)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	idField := importIDField
	vecField := importVecField
	if idField == "" {
		idField = "id"
	}
	if vecField == "" {
		vecField = "vector"
	}

	idIdx := -1
	vecIdx := -1
	for i, h := range headers {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == idField {
			idIdx = i
		}
		if h == vecField {
			vecIdx = i
		}
	}
	if vecIdx < 0 {
		return nil, fmt.Errorf("CSV must have a %q column", vecField)
	}

	var vectors []*pb.VectorWithId
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var id uint64
		if idIdx >= 0 && idIdx < len(row) {
			id, _ = strconv.ParseUint(strings.TrimSpace(row[idIdx]), 10, 64)
		}

		vecStr := strings.TrimSpace(row[vecIdx])
		vecStr = strings.Trim(vecStr, "[]")
		parts := strings.Split(vecStr, ",")
		values := make([]float32, 0, len(parts))
		for _, p := range parts {
			f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
			if err != nil {
				continue
			}
			values = append(values, float32(f))
		}

		vectors = append(vectors, &pb.VectorWithId{
			Id: id,
			Vector: &pb.Vector{
				Values:    values,
				Dimension: uint32(len(values)),
			},
		})
	}
	return vectors, nil
}

func init() {
	importCmd.Flags().StringVar(&importCollection, "collection", "", "Collection name")
	importCmd.Flags().StringVar(&importFile, "file", "", "Input file path")
	importCmd.Flags().StringVar(&importFormat, "format", "", "File format (jsonl, json, csv) — auto-detected from extension")
	importCmd.Flags().IntVar(&importBatchSize, "batch-size", 10000, "Vectors per streaming batch")
	importCmd.Flags().StringVar(&importIDField, "id-field", "id", "CSV column name for vector ID")
	importCmd.Flags().StringVar(&importVecField, "vector-field", "vector", "CSV column name for vector values")

	rootCmd.AddCommand(importCmd)
}
