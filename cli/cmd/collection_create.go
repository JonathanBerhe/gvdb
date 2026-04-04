package cmd

import (
	"fmt"

	pb "gvdb-cli/pb"

	"gvdb-cli/client"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	createName      string
	createDimension uint32
	createMetric    string
	createIndexType string
	createShards    uint32
)

var metricMap = map[string]pb.CreateCollectionRequest_MetricType{
	"l2":            pb.CreateCollectionRequest_L2,
	"inner_product": pb.CreateCollectionRequest_INNER_PRODUCT,
	"ip":            pb.CreateCollectionRequest_INNER_PRODUCT,
	"cosine":        pb.CreateCollectionRequest_COSINE,
}

var indexMap = map[string]pb.CreateCollectionRequest_IndexType{
	"flat":     pb.CreateCollectionRequest_FLAT,
	"hnsw":     pb.CreateCollectionRequest_HNSW,
	"ivf_flat": pb.CreateCollectionRequest_IVF_FLAT,
	"ivf_pq":   pb.CreateCollectionRequest_IVF_PQ,
	"ivf_sq":   pb.CreateCollectionRequest_IVF_SQ,
}

var collectionCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new collection",
	RunE: func(cmd *cobra.Command, args []string) error {
		if createName == "" {
			return fmt.Errorf("--name is required")
		}
		if createDimension == 0 {
			return fmt.Errorf("--dimension is required")
		}

		metric, ok := metricMap[createMetric]
		if !ok {
			return fmt.Errorf("invalid metric %q (l2, inner_product, ip, cosine)", createMetric)
		}
		indexType, ok := indexMap[createIndexType]
		if !ok {
			return fmt.Errorf("invalid index-type %q (flat, hnsw, ivf_flat, ivf_pq, ivf_sq)", createIndexType)
		}

		resp, err := cli.Service.CreateCollection(cli.Ctx(), &pb.CreateCollectionRequest{
			CollectionName: createName,
			Dimension:      createDimension,
			Metric:         metric,
			IndexType:       indexType,
			NumShards:      createShards,
		})
		if err != nil {
			return fmt.Errorf("failed to create collection: %s", client.FormatError(err))
		}

		if outputFormat() == output.JSON {
			output.PrintRaw(output.JSON, map[string]any{
				"collection_id": resp.CollectionId,
				"message":       resp.Message,
			})
			return nil
		}

		fmt.Printf("Created collection %q (id=%d)\n", createName, resp.CollectionId)
		return nil
	},
}

func init() {
	collectionCreateCmd.Flags().StringVar(&createName, "name", "", "Collection name")
	collectionCreateCmd.Flags().Uint32Var(&createDimension, "dimension", 0, "Vector dimension")
	collectionCreateCmd.Flags().StringVar(&createMetric, "metric", "cosine", "Distance metric (l2, inner_product, ip, cosine)")
	collectionCreateCmd.Flags().StringVar(&createIndexType, "index-type", "hnsw", "Index type (flat, hnsw, ivf_flat, ivf_pq, ivf_sq)")
	collectionCreateCmd.Flags().Uint32Var(&createShards, "shards", 0, "Number of shards (0 = default)")

	collectionCmd.AddCommand(collectionCreateCmd)
}
