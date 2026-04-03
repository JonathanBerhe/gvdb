package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	deleteCollection string
	deleteIDs        string
	deleteForce      bool
)

var vectorDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete vectors by ID",
	RunE: func(cmd *cobra.Command, args []string) error {
		if deleteCollection == "" {
			return fmt.Errorf("--collection is required")
		}
		if deleteIDs == "" {
			return fmt.Errorf("--ids is required")
		}

		ids, err := parseIDList(deleteIDs)
		if err != nil {
			return err
		}

		if !deleteForce {
			fmt.Printf("Delete %d vector(s) from %q? [y/N] ", len(ids), deleteCollection)
			reader := bufio.NewReader(os.Stdin)
			answer, _ := reader.ReadString('\n')
			if strings.TrimSpace(strings.ToLower(answer)) != "y" {
				fmt.Println("Aborted.")
				return nil
			}
		}

		resp, err := cli.Service.Delete(cli.Ctx(), &pb.DeleteRequest{
			CollectionName: deleteCollection,
			Ids:            ids,
		})
		if err != nil {
			return fmt.Errorf("delete failed: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			result := map[string]any{
				"deleted_count": resp.DeletedCount,
				"message":       resp.Message,
			}
			if len(resp.NotFoundIds) > 0 {
				result["not_found_ids"] = resp.NotFoundIds
			}
			output.PrintRaw(output.JSON, result)
			return nil
		}

		fmt.Printf("Deleted %d vector(s)\n", resp.DeletedCount)
		if len(resp.NotFoundIds) > 0 {
			fmt.Printf("Not found: %v\n", resp.NotFoundIds)
		}
		return nil
	},
}

func init() {
	vectorDeleteCmd.Flags().StringVar(&deleteCollection, "collection", "", "Collection name")
	vectorDeleteCmd.Flags().StringVar(&deleteIDs, "ids", "", "Comma-separated vector IDs")
	vectorDeleteCmd.Flags().BoolVar(&deleteForce, "force", false, "Skip confirmation prompt")

	vectorCmd.AddCommand(vectorDeleteCmd)
}
