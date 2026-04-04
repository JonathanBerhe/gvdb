package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show server statistics",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := cli.Service.GetStats(cli.Ctx(), &pb.GetStatsRequest{})
		if err != nil {
			return fmt.Errorf("failed to get stats: %s", client.FormatError(err))
		}

		f := outputFormat()
		if f == output.JSON {
			output.PrintRaw(output.JSON, map[string]any{
				"total_collections": resp.TotalCollections,
				"total_vectors":     resp.TotalVectors,
				"total_queries":     resp.TotalQueries,
				"avg_query_time_ms": resp.AvgQueryTimeMs,
			})
			return nil
		}

		output.Print(f, output.TableData{
			Headers: []string{"metric", "value"},
			Rows: [][]string{
				{"Collections", fmt.Sprintf("%d", resp.TotalCollections)},
				{"Vectors", fmt.Sprintf("%d", resp.TotalVectors)},
				{"Queries", fmt.Sprintf("%d", resp.TotalQueries)},
				{"Avg Query Time", fmt.Sprintf("%.2fms", resp.AvgQueryTimeMs)},
			},
		})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)
}
