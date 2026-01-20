package config

import (
	"github.com/BurntSushi/toml"
)

type AppConfig struct {
	Database DatabaseParams `toml:"database"`
	Server   ServerConfig   `toml:"server"`
}

type DatabaseParams struct {
	FilePath   string      `toml:"file_path"`
	Dim        int         `toml:"dim"`
	MetricType string      `toml:"metric_type"`
	IndexType  string      `toml:"index_type"`
	HnswParams *HnswParams `toml:"hnsw_params,omitempty"`
}

type HnswParams struct {
	EFConstruction int `toml:"ef_construction"`
	M              int `toml:"m"`
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
