package observability

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGrafanaDashboardAsset(t *testing.T) {
	data := GrafanaDashboard()
	if !json.Valid(data) {
		t.Fatal("grafana dashboard is not valid JSON")
	}

	var dashboard struct {
		Title  string `json:"title"`
		UID    string `json:"uid"`
		Panels []struct {
			Title string `json:"title"`
			Type  string `json:"type"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatalf("decode dashboard: %v", err)
	}
	if dashboard.Title != "Distributed Cache - Observability" {
		t.Errorf("dashboard title = %q", dashboard.Title)
	}
	if dashboard.UID == "" {
		t.Error("dashboard UID is empty")
	}
	if len(dashboard.Panels) < 10 {
		t.Errorf("dashboard has %d panels, want at least 10", len(dashboard.Panels))
	}

	titles := make(map[string]bool, len(dashboard.Panels))
	for _, panel := range dashboard.Panels {
		titles[panel.Title] = true
	}
	for _, want := range []string{
		"Requests per second",
		"Cache hit ratio",
		"Request latency p95",
		"Request latency p99",
		"Cache hits and misses",
		"Evictions per second",
		"Replication lag",
		"Node availability",
	} {
		if !titles[want] {
			t.Errorf("dashboard is missing panel %q", want)
		}
	}

	body := string(data)
	for _, want := range []string{
		"requests_total",
		"cache_hits",
		"cache_misses",
		"evictions",
		"request_duration_seconds_bucket",
		"replication_lag_seconds",
		"node_up",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard does not query %q", want)
		}
	}
}

func TestAlertRulesAsset(t *testing.T) {
	alerts := string(AlertRules())
	for _, want := range []string{
		"HighRequestLatencyP95",
		"HighRequestLatencyP99",
		"ReplicationLagHigh",
		"ReplicationLagCritical",
		"NodeDown",
		"AllCacheNodesDown",
		"HighRequestErrorRate",
		"histogram_quantile(0.95",
		"replication_lag_seconds",
		"node_up",
		"severity: critical",
	} {
		if !strings.Contains(alerts, want) {
			t.Errorf("alert rules do not contain %q", want)
		}
	}
}

func TestPrometheusConfigAsset(t *testing.T) {
	config := string(PrometheusConfig())
	for _, want := range []string{"alerts.yml", "/metrics", "distributed-cache", "localhost:9090"} {
		if !strings.Contains(config, want) {
			t.Errorf("prometheus config does not contain %q", want)
		}
	}
}

func TestWriteAssets(t *testing.T) {
	dir := t.TempDir()
	if err := WriteAssets(dir); err != nil {
		t.Fatalf("WriteAssets() returned error: %v", err)
	}

	for name, want := range map[string][]byte{
		AssetGrafanaDashboard: GrafanaDashboard(),
		AssetAlertRules:       AlertRules(),
		AssetPrometheusConfig: PrometheusConfig(),
	} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read written asset %q: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("written asset %q does not match bundled asset", name)
		}
	}

	if err := WriteAssets(""); err == nil {
		t.Fatal("WriteAssets(\"\") returned nil error")
	}
}

func TestAssetByName(t *testing.T) {
	if len(AssetNames()) != 3 {
		t.Fatalf("AssetNames() returned %d names, want 3", len(AssetNames()))
	}

	for _, name := range AssetNames() {
		if _, err := Asset(name); err != nil {
			t.Errorf("Asset(%q) returned error: %v", name, err)
		}
	}
	if _, err := Asset("missing.json"); err == nil {
		t.Fatal("Asset(missing.json) returned nil error")
	}
}
