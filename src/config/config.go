package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Broker config holds the broker section of the yaml
type BrokerConfig struct {
	Type    string            `yaml:"type"`
	Name    string            `yaml:"name"`
	Options map[string]string `yaml:"options"`
}

// Config is the root configuration object
type Config struct {
	Broker BrokerConfig `yaml:"broker"`
}

// Read a YAML file configuration
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)

	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validate performs basic checks
func (c *Config) Validate() error {
	if c.Broker.Type == "" {
		return fmt.Errorf("broker.type is required")
	}
	return nil
}
