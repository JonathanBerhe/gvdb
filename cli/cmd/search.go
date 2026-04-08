package cmd

import (
	"encoding/json"
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	searchCollection   string
	searchVector       string
	searchTopK         uint32
	searchFilter       string
	searchMetadata     bool
	searchHybrid       bool
	searchTextQuery    string
	searchVectorWeight float32
	searchTextWeight   float32
	searchTextField    string
)

var searchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search for similar vectors",
	Long:  "Execute vector similarity search or hybrid search (BM25 + vector) with --hybrid flag.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if searchCollection == "" {
			return fmt.Errorf("--collection is required")
		}

		if searchHybrid || searchTextQuery != "" {
			return runHybridSearch()
		}
		return runVectorSearch()
	},
}

func runVectorSearch() error {
	if searchVector == "" {
		return fmt.Errorf("--vector is required for vector search")
	}

	var values []float32
	if err := json.Unmarshal([]byte(searchVector), &values); err != nil {
		return fmt.Errorf("--vector must be a JSON array of floats: %w", err)
	}

	resp, err := cli.Service.Search(cli.Ctx(), &pb.SearchRequest{
		CollectionName: searchCollection,
		QueryVector: &pb.Vector{
			Values:    values,
			Dimension: uint32(len(values)),
		},
		TopK:           searchTopK,
		Filter:         searchFilter,
		ReturnMetadata: searchMetadata,
	})
	if err != nil {
		return fmt.Errorf("search failed: %s", client.FormatError(err))
	}

	return printSearchResults(resp.Results, resp.QueryTimeMs)
}

func runHybridSearch() error {
	req := &pb.HybridSearchRequest{
		CollectionName: searchCollection,
		TextQuery:      searchTextQuery,
		TopK:           searchTopK,
		VectorWeight:   searchVectorWeight,
		TextWeight:     searchTextWeight,
		TextField:      searchTextField,
		Filter:         searchFilter,
		ReturnMetadata: searchMetadata,
	}

	if searchVector != "" {
		var values []float32
		if err := json.Unmarshal([]byte(searchVector), &values); err != nil {
			return fmt.Errorf("--vector must be a JSON array of floats: %w", err)
		}
		req.QueryVector = &pb.Vector{
			Values:    values,
			Dimension: uint32(len(values)),
		}
	}

	if req.QueryVector == nil && req.TextQuery == "" {
		return fmt.Errorf("hybrid search requires --vector and/or --text-query")
	}

	resp, err := cli.Service.HybridSearch(cli.Ctx(), req)
	if err != nil {
		return fmt.Errorf("hybrid search failed: %s", client.FormatError(err))
	}

	return printSearchResults(resp.Results, resp.QueryTimeMs)
}

func printSearchResults(results []*pb.SearchResultEntry, queryTimeMs float32) error {
	f := outputFormat()
	if f == output.JSON {
		items := make([]map[string]any, 0, len(results))
		for _, r := range results {
			entry := map[string]any{
				"id":       r.Id,
				"distance": r.Distance,
			}
			if r.Metadata != nil {
				entry["metadata"] = metadataToMap(r.Metadata)
			}
			items = append(items, entry)
		}
		output.PrintRaw(output.JSON, map[string]any{
			"results":       items,
			"query_time_ms": queryTimeMs,
		})
		return nil
	}

	rows := make([][]string, 0, len(results))
	for i, r := range results {
		rows = append(rows, []string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("%d", r.Id),
			fmt.Sprintf("%.6f", r.Distance),
			metadataString(r.Metadata),
		})
	}

	output.Print(f, output.TableData{
		Headers: []string{"#", "id", "distance", "metadata"},
		Rows:    rows,
	})

	fmt.Printf("\n%d results in %.2fms\n", len(results), queryTimeMs)
	return nil
}

func init() {
	searchCmd.Flags().StringVar(&searchCollection, "collection", "", "Collection name")
	searchCmd.Flags().StringVar(&searchVector, "vector", "", "Query vector as JSON array")
	searchCmd.Flags().Uint32Var(&searchTopK, "top-k", 10, "Number of results")
	searchCmd.Flags().StringVar(&searchFilter, "filter", "", "Metadata filter expression")
	searchCmd.Flags().BoolVar(&searchMetadata, "metadata", true, "Return metadata with results")
	searchCmd.Flags().BoolVar(&searchHybrid, "hybrid", false, "Enable hybrid search (BM25 + vector)")
	searchCmd.Flags().StringVar(&searchTextQuery, "text-query", "", "Text query for BM25 (implies --hybrid)")
	searchCmd.Flags().Float32Var(&searchVectorWeight, "vector-weight", 0.5, "Vector weight for hybrid search")
	searchCmd.Flags().Float32Var(&searchTextWeight, "text-weight", 0.5, "Text weight for hybrid search")
	searchCmd.Flags().StringVar(&searchTextField, "text-field", "text", "Metadata field for BM25 text search")

	rootCmd.AddCommand(searchCmd)
}
