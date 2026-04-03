package config

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Address  string             `yaml:"address"`
	APIKey   string             `yaml:"api_key"`
	Output   string             `yaml:"output"`
	Timeout  time.Duration      `yaml:"timeout"`
	NoColor  bool               `yaml:"no_color"`
	Profile  string             `yaml:"profile"`
	Profiles map[string]Profile `yaml:"profiles"`
}

type Profile struct {
	Address string `yaml:"address"`
	APIKey  string `yaml:"api_key"`
}

func DefaultPath() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "gvdb", "config.yaml")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "gvdb", "config.yaml")
}

func Load(path string) *Config {
	cfg := &Config{
		Address: "localhost:50051",
		Output:  "table",
		Timeout: 30 * time.Second,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg
	}

	_ = yaml.Unmarshal(data, cfg)

	// Apply active profile
	if cfg.Profile != "" {
		if p, ok := cfg.Profiles[cfg.Profile]; ok {
			if p.Address != "" {
				cfg.Address = p.Address
			}
			if p.APIKey != "" {
				cfg.APIKey = p.APIKey
			}
		}
	}

	return cfg
}

func (c *Config) Resolve(address, apiKey, output, profile string, timeout time.Duration, noColor bool) {
	// Apply named profile first
	if profile != "" {
		if p, ok := c.Profiles[profile]; ok {
			if p.Address != "" {
				c.Address = p.Address
			}
			if p.APIKey != "" {
				c.APIKey = p.APIKey
			}
		}
	}

	// Env vars override config
	if addr := os.Getenv("GVDB_ADDR"); addr != "" {
		c.Address = addr
	}
	if key := os.Getenv("GVDB_API_KEY"); key != "" {
		c.APIKey = key
	}

	// Flags override everything
	if address != "" {
		c.Address = address
	}
	if apiKey != "" {
		c.APIKey = apiKey
	}
	if output != "" {
		c.Output = output
	}
	if timeout != 0 {
		c.Timeout = timeout
	}
	if noColor {
		c.NoColor = noColor
	}
}
