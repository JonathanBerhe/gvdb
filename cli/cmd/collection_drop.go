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

var dropForce bool

var collectionDropCmd = &cobra.Command{
	Use:   "drop <name>",
	Short: "Drop a collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]

		if !dropForce {
			fmt.Printf("Drop collection %q? This cannot be undone. [y/N] ", name)
			reader := bufio.NewReader(os.Stdin)
			answer, _ := reader.ReadString('\n')
			if strings.TrimSpace(strings.ToLower(answer)) != "y" {
				fmt.Println("Aborted.")
				return nil
			}
		}

		_, err := cli.Service.DropCollection(cli.Ctx(), &pb.DropCollectionRequest{
			CollectionName: name,
		})
		if err != nil {
			return fmt.Errorf("failed to drop collection: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]string{"message": "Collection dropped"})
			return nil
		}

		fmt.Printf("Dropped collection %q\n", name)
		return nil
	},
}

func init() {
	collectionDropCmd.Flags().BoolVar(&dropForce, "force", false, "Skip confirmation prompt")
	collectionCmd.AddCommand(collectionDropCmd)
}
