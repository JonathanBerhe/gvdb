package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
)

//go:embed static/*
var staticFiles embed.FS

func main() {
	gvdbAddr := flag.String("gvdb-addr", "localhost:50051", "GVDB gRPC server address")
	port := flag.Int("port", 8080, "HTTP port for the web UI")
	apiKey := flag.String("api-key", "", "Optional API key for GVDB authentication")
	flag.Parse()

	// Override from env
	if addr := os.Getenv("GVDB_ADDR"); addr != "" {
		*gvdbAddr = addr
	}
	if key := os.Getenv("GVDB_API_KEY"); key != "" {
		*apiKey = key
	}

	// Create gRPC connection to GVDB
	gateway, err := NewGateway(*gvdbAddr, *apiKey)
	if err != nil {
		log.Fatalf("Failed to connect to GVDB at %s: %v", *gvdbAddr, err)
	}
	defer gateway.Close()

	mux := http.NewServeMux()

	// REST API routes
	mux.HandleFunc("GET /api/health", gateway.HandleHealth)
	mux.HandleFunc("GET /api/stats", gateway.HandleStats)
	mux.HandleFunc("GET /api/collections", gateway.HandleListCollections)
	mux.HandleFunc("POST /api/collections", gateway.HandleCreateCollection)
	mux.HandleFunc("DELETE /api/collections/{name}", gateway.HandleDropCollection)
	mux.HandleFunc("GET /api/collections/{name}/vectors", gateway.HandleListVectors)
	mux.HandleFunc("POST /api/collections/{name}/search", gateway.HandleSearch)
	mux.HandleFunc("POST /api/collections/{name}/hybrid-search", gateway.HandleHybridSearch)
	mux.HandleFunc("GET /api/metrics", gateway.HandleMetrics)

	// Serve React SPA (embedded static files)
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatalf("Failed to create static FS: %v", err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.Handle("/", spaHandler(fileServer))

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	log.Printf("GVDB Web UI starting on http://%s (backend: %s)", addr, *gvdbAddr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// spaHandler wraps a file server to serve index.html for non-file routes (React Router)
func spaHandler(fileServer http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try serving the file; if it doesn't exist, serve index.html
		path := r.URL.Path
		if path != "/" && path != "" {
			// Check if the file exists in the embedded FS
			if _, err := fs.Stat(staticFiles, "static"+path); err == nil {
				fileServer.ServeHTTP(w, r)
				return
			}
		}
		// Serve index.html for SPA routing
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
