package cmd

import (
	"github.com/spf13/cobra"
)

var collectionCmd = &cobra.Command{
	Use:     "collection",
	Aliases: []string{"col"},
	Short:   "Manage collections",
}

func init() {
	rootCmd.AddCommand(collectionCmd)
}
