package observability

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
)

// Bundled observability asset names.
const (
	AssetGrafanaDashboard = "grafana-dashboard.json"
	AssetAlertRules       = "alerts.yml"
	AssetPrometheusConfig = "prometheus.yml"
)

// assetsFS contains the Grafana dashboard, Prometheus alert rules, and a
// minimal Prometheus scrape configuration.
//
//go:embed assets/*
var assetsFS embed.FS

// GrafanaDashboard returns the bundled Grafana dashboard JSON.
func GrafanaDashboard() []byte {
	return mustAsset(AssetGrafanaDashboard)
}

// AlertRules returns the bundled Prometheus alert rules.
func AlertRules() []byte {
	return mustAsset(AssetAlertRules)
}

// PrometheusConfig returns the bundled Prometheus scrape configuration.
func PrometheusConfig() []byte {
	return mustAsset(AssetPrometheusConfig)
}

// Asset returns one bundled observability asset by name.
func Asset(name string) ([]byte, error) {
	switch name {
	case AssetGrafanaDashboard, AssetAlertRules, AssetPrometheusConfig:
		data, err := assetsFS.ReadFile("assets/" + name)
		if err != nil {
			return nil, fmt.Errorf("observability: read asset %q: %w", name, err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("observability: unknown asset %q", name)
}

// AssetNames returns the names of all bundled observability assets.
func AssetNames() []string {
	return []string{AssetAlertRules, AssetGrafanaDashboard, AssetPrometheusConfig}
}

// WriteAssets writes every bundled asset into dir, creating the directory when
// needed. Existing files with the same names are overwritten.
func WriteAssets(dir string) error {
	if dir == "" {
		return fmt.Errorf("observability: asset directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("observability: create asset directory: %w", err)
	}

	for _, name := range AssetNames() {
		data, err := Asset(name)
		if err != nil {
			return err
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return fmt.Errorf("observability: write asset %q: %w", path, err)
		}
	}
	return nil
}

// mustAsset reads an embedded file that is guaranteed to exist at build time.
func mustAsset(name string) []byte {
	data, err := Asset(name)
	if err != nil {
		panic(err)
	}
	return data
}
