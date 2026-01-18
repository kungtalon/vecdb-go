package config

import (
	"github.com/BurntSushi/toml"
)

type AppConfig struct {
	Database DatabaseParams `toml:"database"`
	FilePath string         `toml:"file_path"`
	Server   ServerConfig   `toml:"server"`
}

type DatabaseParams struct {
	// Define the fields based on your database configuration
}

type ServerConfig struct {
	SearchURLSuffix string `toml:"search_url_suffix"`
	UpsertURLSuffix string `toml:"upsert_url_suffix"`
	Port            uint16 `toml:"port"`
	LogLevel        string `toml:"log_level"`
}

func LoadConfig() (*AppConfig, error) {
	var config AppConfig
	if _, err := toml.DecodeFile("config.toml", &config); err != nil {
		return nil, err
	}
	return &config, nil
}
