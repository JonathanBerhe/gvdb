package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	listVecCollection string
	listVecLimit      uint32
	listVecOffset     uint64
	listVecMetadata   bool
)

var vectorListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List vectors in a collection",
	RunE: func(cmd *cobra.Command, args []string) error {
		if listVecCollection == "" {
			return fmt.Errorf("--collection is required")
		}

		resp, err := cli.Service.ListVectors(cli.Ctx(), &pb.ListVectorsRequest{
			CollectionName:  listVecCollection,
			Limit:           listVecLimit,
			Offset:          listVecOffset,
			IncludeMetadata: listVecMetadata,
		})
		if err != nil {
			return fmt.Errorf("failed to list vectors: %s", client.FormatError(err))
		}

		f := outputFormat()
		if f == output.JSON {
			items := make([]map[string]any, 0, len(resp.Vectors))
			for _, v := range resp.Vectors {
				entry := map[string]any{
					"id":        v.Id,
					"dimension": v.Vector.Dimension,
				}
				if v.Metadata != nil {
					entry["metadata"] = metadataToMap(v.Metadata)
				}
				items = append(items, entry)
			}
			output.PrintRaw(output.JSON, map[string]any{
				"vectors":     items,
				"total_count": resp.TotalCount,
				"has_more":    resp.HasMore,
			})
			return nil
		}

		rows := make([][]string, 0, len(resp.Vectors))
		for _, v := range resp.Vectors {
			rows = append(rows, []string{
				fmt.Sprintf("%d", v.Id),
				fmt.Sprintf("%d", v.Vector.Dimension),
				metadataString(v.Metadata),
			})
		}

		output.Print(f, output.TableData{
			Headers: []string{"id", "dimension", "metadata"},
			Rows:    rows,
		})

		fmt.Printf("\n%d–%d of %d", listVecOffset+1, listVecOffset+uint64(len(resp.Vectors)), resp.TotalCount)
		if resp.HasMore {
			fmt.Printf(" (more available)")
		}
		fmt.Println()
		return nil
	},
}

func init() {
	vectorListCmd.Flags().StringVar(&listVecCollection, "collection", "", "Collection name")
	vectorListCmd.Flags().Uint32Var(&listVecLimit, "limit", 20, "Page size")
	vectorListCmd.Flags().Uint64Var(&listVecOffset, "offset", 0, "Offset")
	vectorListCmd.Flags().BoolVar(&listVecMetadata, "metadata", true, "Include metadata")

	vectorCmd.AddCommand(vectorListCmd)
}
