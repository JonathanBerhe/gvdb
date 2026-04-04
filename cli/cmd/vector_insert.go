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
	insertCollection string
	insertID         uint64
	insertValues     string
	insertMetadata   string
)

var vectorInsertCmd = &cobra.Command{
	Use:   "insert",
	Short: "Insert a vector",
	RunE: func(cmd *cobra.Command, args []string) error {
		if insertCollection == "" {
			return fmt.Errorf("--collection is required")
		}

		var values []float32
		if err := json.Unmarshal([]byte(insertValues), &values); err != nil {
			return fmt.Errorf("--values must be a JSON array of floats: %w", err)
		}
		if len(values) == 0 {
			return fmt.Errorf("--values must not be empty")
		}

		md, err := parseMetadataFlag(insertMetadata)
		if err != nil {
			return err
		}

		vec := &pb.VectorWithId{
			Id: insertID,
			Vector: &pb.Vector{
				Values:    values,
				Dimension: uint32(len(values)),
			},
			Metadata: md,
		}

		resp, err := cli.Service.Insert(cli.Ctx(), &pb.InsertRequest{
			CollectionName: insertCollection,
			Vectors:        []*pb.VectorWithId{vec},
		})
		if err != nil {
			return fmt.Errorf("insert failed: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]any{
				"inserted_count": resp.InsertedCount,
				"message":        resp.Message,
			})
			return nil
		}

		fmt.Printf("Inserted %d vector(s)\n", resp.InsertedCount)
		return nil
	},
}

func init() {
	vectorInsertCmd.Flags().StringVar(&insertCollection, "collection", "", "Collection name")
	vectorInsertCmd.Flags().Uint64Var(&insertID, "id", 0, "Vector ID")
	vectorInsertCmd.Flags().StringVar(&insertValues, "values", "", "Vector values as JSON array (e.g. [0.1,0.2,0.3])")
	vectorInsertCmd.Flags().StringVar(&insertMetadata, "metadata", "", `Metadata as JSON (e.g. '{"key":"value"}')`)

	vectorCmd.AddCommand(vectorInsertCmd)
}
