package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check server health",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := cli.Service.HealthCheck(cli.Ctx(), &pb.HealthCheckRequest{})
		if err != nil {
			return fmt.Errorf("health check failed: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]string{
				"status":  resp.Status.String(),
				"message": resp.Message,
			})
			return nil
		}

		status := resp.Status.String()
		if resp.Status == pb.HealthCheckResponse_SERVING {
			fmt.Printf("SERVING (%s)\n", cli.Address())
		} else {
			fmt.Printf("%s: %s\n", status, resp.Message)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(healthCmd)
}
