package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadMissing(t *testing.T) {
	cfg := Load("/nonexistent/path/config.yaml")
	if cfg.Address != "localhost:50051" {
		t.Errorf("expected default address, got %s", cfg.Address)
	}
	if cfg.Output != "table" {
		t.Errorf("expected default output, got %s", cfg.Output)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("expected 30s timeout, got %s", cfg.Timeout)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
address: staging:50051
output: json
timeout: 10s
profiles:
  prod:
    address: prod:50051
    api_key: prod-key
`), 0644)

	cfg := Load(path)
	if cfg.Address != "staging:50051" {
		t.Errorf("expected staging:50051, got %s", cfg.Address)
	}
	if cfg.Output != "json" {
		t.Errorf("expected json, got %s", cfg.Output)
	}
}

func TestResolveProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
address: default:50051
profiles:
  prod:
    address: prod:50051
    api_key: prod-key
`), 0644)

	cfg := Load(path)
	cfg.Resolve("", "", "", "prod", 0, false)

	if cfg.Address != "prod:50051" {
		t.Errorf("expected prod:50051, got %s", cfg.Address)
	}
	if cfg.APIKey != "prod-key" {
		t.Errorf("expected prod-key, got %s", cfg.APIKey)
	}
}

func TestResolveFlagsOverride(t *testing.T) {
	cfg := Load("/nonexistent")
	cfg.Resolve("custom:9999", "my-key", "csv", "", 5*time.Second, true)

	if cfg.Address != "custom:9999" {
		t.Errorf("expected custom:9999, got %s", cfg.Address)
	}
	if cfg.APIKey != "my-key" {
		t.Errorf("expected my-key, got %s", cfg.APIKey)
	}
	if cfg.Output != "csv" {
		t.Errorf("expected csv, got %s", cfg.Output)
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("expected 5s, got %s", cfg.Timeout)
	}
	if !cfg.NoColor {
		t.Error("expected NoColor true")
	}
}

func TestResolveEnvOverride(t *testing.T) {
	os.Setenv("GVDB_ADDR", "env-addr:50051")
	os.Setenv("GVDB_API_KEY", "env-key")
	defer os.Unsetenv("GVDB_ADDR")
	defer os.Unsetenv("GVDB_API_KEY")

	cfg := Load("/nonexistent")
	cfg.Resolve("", "", "", "", 0, false)

	if cfg.Address != "env-addr:50051" {
		t.Errorf("expected env-addr:50051, got %s", cfg.Address)
	}
	if cfg.APIKey != "env-key" {
		t.Errorf("expected env-key, got %s", cfg.APIKey)
	}
}
