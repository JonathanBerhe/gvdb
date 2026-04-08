package cmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	getCollection     string
	getIDs            string
	getReturnMetadata bool
)

var vectorGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get vectors by ID",
	RunE: func(cmd *cobra.Command, args []string) error {
		if getCollection == "" {
			return fmt.Errorf("--collection is required")
		}
		if getIDs == "" {
			return fmt.Errorf("--ids is required")
		}

		ids, err := parseIDList(getIDs)
		if err != nil {
			return err
		}

		resp, err := cli.Service.Get(cli.Ctx(), &pb.GetRequest{
			CollectionName: getCollection,
			Ids:            ids,
			ReturnMetadata: getReturnMetadata,
		})
		if err != nil {
			return fmt.Errorf("get failed: %s", client.FormatError(err))
		}

		f := outputFormat()
		if f == output.JSON {
			items := make([]map[string]any, 0, len(resp.Vectors))
			for _, v := range resp.Vectors {
				entry := map[string]any{
					"id":        v.Id,
					"dimension": v.Vector.Dimension,
					"values":    v.Vector.Values,
				}
				if v.Metadata != nil {
					entry["metadata"] = metadataToMap(v.Metadata)
				}
				items = append(items, entry)
			}
			result := map[string]any{"vectors": items}
			if len(resp.NotFoundIds) > 0 {
				result["not_found_ids"] = resp.NotFoundIds
			}
			output.PrintRaw(output.JSON, result)
			return nil
		}

		rows := make([][]string, 0, len(resp.Vectors))
		for _, v := range resp.Vectors {
			valStr := "[]"
			if v.Vector != nil {
				b, _ := json.Marshal(v.Vector.Values)
				valStr = string(b)
				if len(valStr) > 60 {
					valStr = valStr[:57] + "..."
				}
			}
			rows = append(rows, []string{
				fmt.Sprintf("%d", v.Id),
				fmt.Sprintf("%d", v.Vector.Dimension),
				valStr,
				metadataString(v.Metadata),
			})
		}

		output.Print(f, output.TableData{
			Headers: []string{"id", "dimension", "values", "metadata"},
			Rows:    rows,
		})

		if len(resp.NotFoundIds) > 0 {
			fmt.Printf("Not found: %v\n", resp.NotFoundIds)
		}
		return nil
	},
}

func parseIDList(s string) ([]uint64, error) {
	parts := strings.Split(s, ",")
	ids := make([]uint64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid ID %q: %w", p, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func init() {
	vectorGetCmd.Flags().StringVar(&getCollection, "collection", "", "Collection name")
	vectorGetCmd.Flags().StringVar(&getIDs, "ids", "", "Comma-separated vector IDs")
	vectorGetCmd.Flags().BoolVar(&getReturnMetadata, "metadata", true, "Return metadata")

	vectorCmd.AddCommand(vectorGetCmd)
}
