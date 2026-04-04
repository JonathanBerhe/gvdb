package cmd

import (
	"encoding/json"
	"fmt"

	pb "gvdb-cli/pb"

	"github.com/spf13/cobra"
)

var vectorCmd = &cobra.Command{
	Use:     "vector",
	Aliases: []string{"vec"},
	Short:   "Manage vectors",
}

func init() {
	rootCmd.AddCommand(vectorCmd)
}

func parseMetadataFlag(raw string) (*pb.Metadata, error) {
	if raw == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("invalid metadata JSON: %w", err)
	}
	return mapToMetadata(m), nil
}

func mapToMetadata(m map[string]any) *pb.Metadata {
	fields := make(map[string]*pb.MetadataValue)
	for k, v := range m {
		switch val := v.(type) {
		case float64:
			if val == float64(int64(val)) {
				fields[k] = &pb.MetadataValue{Value: &pb.MetadataValue_IntValue{IntValue: int64(val)}}
			} else {
				fields[k] = &pb.MetadataValue{Value: &pb.MetadataValue_DoubleValue{DoubleValue: val}}
			}
		case string:
			fields[k] = &pb.MetadataValue{Value: &pb.MetadataValue_StringValue{StringValue: val}}
		case bool:
			fields[k] = &pb.MetadataValue{Value: &pb.MetadataValue_BoolValue{BoolValue: val}}
		}
	}
	return &pb.Metadata{Fields: fields}
}

func metadataToMap(md *pb.Metadata) map[string]any {
	if md == nil {
		return nil
	}
	m := make(map[string]any)
	for k, v := range md.Fields {
		switch x := v.Value.(type) {
		case *pb.MetadataValue_IntValue:
			m[k] = x.IntValue
		case *pb.MetadataValue_DoubleValue:
			m[k] = x.DoubleValue
		case *pb.MetadataValue_StringValue:
			m[k] = x.StringValue
		case *pb.MetadataValue_BoolValue:
			m[k] = x.BoolValue
		}
	}
	return m
}

func metadataString(md *pb.Metadata) string {
	m := metadataToMap(md)
	if m == nil {
		return ""
	}
	b, _ := json.Marshal(m)
	return string(b)
}
