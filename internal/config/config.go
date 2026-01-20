package config

import (
	"fmt"
	"vecdb-go/internal/common"

	"github.com/BurntSushi/toml"
)

type AppConfig struct {
	Database common.DatabaseParams `toml:"database"`
	Server   ServerConfig          `toml:"server"`
}

type ProfileConfig struct {
	Dev  AppConfig `toml:"dev"`
	Test AppConfig `toml:"test"`
}

type ServerConfig struct {
	SearchURLSuffix string `toml:"search_url_suffix"`
	UpsertURLSuffix string `toml:"upsert_url_suffix"`
	Port            uint16 `toml:"port"`
	LogLevel        string `toml:"log_level"`
}

func LoadConfig() (*AppConfig, error) {
	return LoadConfigWithProfile("dev")
}

func LoadConfigWithProfile(profile string) (*AppConfig, error) {
	var profileConfig ProfileConfig
	if _, err := toml.DecodeFile("config.toml", &profileConfig); err != nil {
		return nil, err
	}

	switch profile {
	case "dev":
		return &profileConfig.Dev, nil
	case "test":
		return &profileConfig.Test, nil
	default:
		return nil, fmt.Errorf("unknown profile: %s", profile)
	}
}
