package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	pb "gvdb-ui/pb"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// GET /api/health
func (g *Gateway) HandleHealth(w http.ResponseWriter, r *http.Request) {
	resp, err := g.client.HealthCheck(g.ctx(), &pb.HealthCheckRequest{})
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"status":  resp.Status.String(),
		"message": resp.Message,
	})
}

// GET /api/stats
func (g *Gateway) HandleStats(w http.ResponseWriter, r *http.Request) {
	resp, err := g.client.GetStats(g.ctx(), &pb.GetStatsRequest{})
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"total_collections": resp.TotalCollections,
		"total_vectors":     resp.TotalVectors,
		"total_queries":     resp.TotalQueries,
		"avg_query_time_ms": resp.AvgQueryTimeMs,
	})
}

// GET /api/collections
func (g *Gateway) HandleListCollections(w http.ResponseWriter, r *http.Request) {
	resp, err := g.client.ListCollections(g.ctx(), &pb.ListCollectionsRequest{})
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	collections := make([]map[string]any, 0, len(resp.Collections))
	for _, c := range resp.Collections {
		collections = append(collections, map[string]any{
			"id":           c.CollectionId,
			"name":         c.CollectionName,
			"dimension":    c.Dimension,
			"vector_count": c.VectorCount,
		})
	}
	writeJSON(w, 200, collections)
}

// POST /api/collections
func (g *Gateway) HandleCreateCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name      string `json:"name"`
		Dimension uint32 `json:"dimension"`
		Metric    string `json:"metric"`
		IndexType string `json:"index_type"`
		NumShards uint32 `json:"num_shards"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "Invalid JSON: "+err.Error())
		return
	}

	metricMap := map[string]pb.CreateCollectionRequest_MetricType{
		"l2":            pb.CreateCollectionRequest_L2,
		"inner_product": pb.CreateCollectionRequest_INNER_PRODUCT,
		"cosine":        pb.CreateCollectionRequest_COSINE,
	}
	indexMap := map[string]pb.CreateCollectionRequest_IndexType{
		"flat":     pb.CreateCollectionRequest_FLAT,
		"hnsw":     pb.CreateCollectionRequest_HNSW,
		"ivf_flat": pb.CreateCollectionRequest_IVF_FLAT,
		"ivf_pq":   pb.CreateCollectionRequest_IVF_PQ,
		"ivf_sq":   pb.CreateCollectionRequest_IVF_SQ,
	}

	metric, ok := metricMap[body.Metric]
	if !ok {
		metric = pb.CreateCollectionRequest_L2
	}
	indexType, ok := indexMap[body.IndexType]
	if !ok {
		indexType = pb.CreateCollectionRequest_HNSW
	}

	resp, err := g.client.CreateCollection(g.ctx(), &pb.CreateCollectionRequest{
		CollectionName: body.Name,
		Dimension:      body.Dimension,
		Metric:         metric,
		IndexType:      indexType,
		NumShards:      body.NumShards,
	})
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{
		"collection_id": resp.CollectionId,
		"message":       resp.Message,
	})
}

// DELETE /api/collections/{name}
func (g *Gateway) HandleDropCollection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	_, err := g.client.DropCollection(g.ctx(), &pb.DropCollectionRequest{
		CollectionName: name,
	})
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"message": "Collection dropped"})
}

// GET /api/collections/{name}/vectors?limit=20&offset=0&metadata=true
func (g *Gateway) HandleListVectors(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	includeMeta := r.URL.Query().Get("metadata") == "true"

	if limit <= 0 {
		limit = 20
	}

	resp, err := g.client.ListVectors(g.ctx(), &pb.ListVectorsRequest{
		CollectionName:  name,
		Limit:           uint32(limit),
		Offset:          uint64(offset),
		IncludeMetadata: includeMeta,
	})
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}

	vectors := make([]map[string]any, 0, len(resp.Vectors))
	for _, v := range resp.Vectors {
		entry := map[string]any{
			"id":        v.Id,
			"dimension": v.Vector.Dimension,
		}
		if includeMeta && v.Metadata != nil {
			meta := make(map[string]any)
			for k, val := range v.Metadata.Fields {
				switch x := val.Value.(type) {
				case *pb.MetadataValue_IntValue:
					meta[k] = x.IntValue
				case *pb.MetadataValue_DoubleValue:
					meta[k] = x.DoubleValue
				case *pb.MetadataValue_StringValue:
					meta[k] = x.StringValue
				case *pb.MetadataValue_BoolValue:
					meta[k] = x.BoolValue
				}
			}
			entry["metadata"] = meta
		}
		vectors = append(vectors, entry)
	}

	writeJSON(w, 200, map[string]any{
		"vectors":     vectors,
		"total_count": resp.TotalCount,
		"has_more":    resp.HasMore,
	})
}

// POST /api/collections/{name}/search
func (g *Gateway) HandleSearch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body struct {
		QueryVector    []float32 `json:"query_vector"`
		TopK           uint32    `json:"top_k"`
		Filter         string    `json:"filter"`
		ReturnMetadata bool      `json:"return_metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "Invalid JSON: "+err.Error())
		return
	}
	if body.TopK == 0 {
		body.TopK = 10
	}

	resp, err := g.client.Search(g.ctx(), &pb.SearchRequest{
		CollectionName: name,
		QueryVector:    &pb.Vector{Values: body.QueryVector, Dimension: uint32(len(body.QueryVector))},
		TopK:           body.TopK,
		Filter:         body.Filter,
		ReturnMetadata: body.ReturnMetadata,
	})
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	results := formatSearchResults(resp.Results)
	writeJSON(w, 200, map[string]any{
		"results":       results,
		"query_time_ms": resp.QueryTimeMs,
	})
}

// POST /api/collections/{name}/hybrid-search
func (g *Gateway) HandleHybridSearch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body struct {
		QueryVector    []float32 `json:"query_vector"`
		TextQuery      string    `json:"text_query"`
		TopK           uint32    `json:"top_k"`
		VectorWeight   float32   `json:"vector_weight"`
		TextWeight     float32   `json:"text_weight"`
		TextField      string    `json:"text_field"`
		Filter         string    `json:"filter"`
		ReturnMetadata bool      `json:"return_metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "Invalid JSON: "+err.Error())
		return
	}
	if body.TopK == 0 {
		body.TopK = 10
	}
	if body.TextField == "" {
		body.TextField = "text"
	}

	req := &pb.HybridSearchRequest{
		CollectionName: name,
		TextQuery:      body.TextQuery,
		TopK:           body.TopK,
		VectorWeight:   body.VectorWeight,
		TextWeight:     body.TextWeight,
		TextField:      body.TextField,
		Filter:         body.Filter,
		ReturnMetadata: body.ReturnMetadata,
	}
	if len(body.QueryVector) > 0 {
		req.QueryVector = &pb.Vector{Values: body.QueryVector, Dimension: uint32(len(body.QueryVector))}
	}

	resp, err := g.client.HybridSearch(g.ctx(), req)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	results := formatSearchResults(resp.Results)
	writeJSON(w, 200, map[string]any{
		"results":       results,
		"query_time_ms": resp.QueryTimeMs,
	})
}

func formatSearchResults(entries []*pb.SearchResultEntry) []map[string]any {
	results := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		entry := map[string]any{
			"id":       e.Id,
			"distance": e.Distance,
		}
		if e.Metadata != nil {
			meta := make(map[string]any)
			for k, val := range e.Metadata.Fields {
				switch x := val.Value.(type) {
				case *pb.MetadataValue_IntValue:
					meta[k] = x.IntValue
				case *pb.MetadataValue_DoubleValue:
					meta[k] = x.DoubleValue
				case *pb.MetadataValue_StringValue:
					meta[k] = x.StringValue
				case *pb.MetadataValue_BoolValue:
					meta[k] = x.BoolValue
				}
			}
			entry["metadata"] = meta
		}
		results = append(results, entry)
	}
	return results
}
