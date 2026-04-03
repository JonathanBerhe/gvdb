package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var collectionListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all collections",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := cli.Service.ListCollections(cli.Ctx(), &pb.ListCollectionsRequest{})
		if err != nil {
			return fmt.Errorf("failed to list collections: %s", client.FormatError(err))
		}

		f := outputFormat()
		if f == output.JSON {
			items := make([]map[string]any, 0, len(resp.Collections))
			for _, c := range resp.Collections {
				items = append(items, map[string]any{
					"id":           c.CollectionId,
					"name":         c.CollectionName,
					"dimension":    c.Dimension,
					"metric":       c.MetricType,
					"vector_count": c.VectorCount,
				})
			}
			output.PrintRaw(output.JSON, items)
			return nil
		}

		rows := make([][]string, 0, len(resp.Collections))
		for _, c := range resp.Collections {
			rows = append(rows, []string{
				c.CollectionName,
				fmt.Sprintf("%d", c.CollectionId),
				fmt.Sprintf("%d", c.Dimension),
				c.MetricType,
				fmt.Sprintf("%d", c.VectorCount),
			})
		}

		output.Print(f, output.TableData{
			Headers: []string{"name", "id", "dimension", "metric", "vectors"},
			Rows:    rows,
		})
		return nil
	},
}

func init() {
	collectionCmd.AddCommand(collectionListCmd)
}
