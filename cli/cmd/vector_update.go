package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	updateCollection string
	updateID         uint64
	updateMetadata   string
	updateMerge      bool
)

var vectorUpdateCmd = &cobra.Command{
	Use:   "update-metadata",
	Short: "Update vector metadata",
	RunE: func(cmd *cobra.Command, args []string) error {
		if updateCollection == "" {
			return fmt.Errorf("--collection is required")
		}
		if updateMetadata == "" {
			return fmt.Errorf("--metadata is required")
		}

		md, err := parseMetadataFlag(updateMetadata)
		if err != nil {
			return err
		}

		resp, err := cli.Service.UpdateMetadata(cli.Ctx(), &pb.UpdateMetadataRequest{
			CollectionName: updateCollection,
			Id:             updateID,
			Metadata:       md,
			Merge:          updateMerge,
		})
		if err != nil {
			return fmt.Errorf("update failed: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]any{
				"updated": resp.Updated,
				"message": resp.Message,
			})
			return nil
		}

		if resp.Updated {
			fmt.Printf("Updated metadata for vector %d\n", updateID)
		} else {
			fmt.Printf("Vector %d not found\n", updateID)
		}
		return nil
	},
}

func init() {
	vectorUpdateCmd.Flags().StringVar(&updateCollection, "collection", "", "Collection name")
	vectorUpdateCmd.Flags().Uint64Var(&updateID, "id", 0, "Vector ID")
	vectorUpdateCmd.Flags().StringVar(&updateMetadata, "metadata", "", `Metadata as JSON (e.g. '{"key":"value"}')`)
	vectorUpdateCmd.Flags().BoolVar(&updateMerge, "merge", true, "Merge with existing metadata (false = replace)")

	vectorCmd.AddCommand(vectorUpdateCmd)
}
