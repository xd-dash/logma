package axiom

import (
	"os"
	"strconv"
	"strings"
)

// FromEnv constructs an observer from the runtime environment. It remains
// disabled unless LOGMA_AXIOM_ENABLED=true. Prefer LOGMA_AXIOM_TOKEN_FILE for
// systemd/secret-file delivery; LOGMA_AXIOM_TOKEN is supported for runtimes
// whose secret boundary already uses environment injection.
func FromEnv() *Observer {
	enabled, _ := strconv.ParseBool(os.Getenv("LOGMA_AXIOM_ENABLED"))
	token := ""
	if path := strings.TrimSpace(os.Getenv("LOGMA_AXIOM_TOKEN_FILE")); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			token = strings.TrimSpace(string(data))
		}
	}
	if token == "" {
		token = strings.TrimSpace(os.Getenv("LOGMA_AXIOM_TOKEN"))
	}

	mode := PublishMode(strings.TrimSpace(os.Getenv("LOGMA_AXIOM_PUBLISH_MODE")))
	static := map[string]any{}
	for env, field := range map[string]string{
		"FATLINE_ID":                "fatline_id",
		"FATLINE_SESSION_ID":        "fatline_session_id",
		"FATLINE_SCOPE":             "fatline_scope",
		"HURAM_DEPLOYMENT_ID":       "deployment_id",
		"HURAM_COMPOSITION_ID":      "composition_id",
		"HURAM_RUN_ID":              "huram_run_id",
		"RUNTIME_MODE":              "mode",
		"CLOUD_REGION":              "region",
		"SOURCE_SHA":                "source_sha",
		"LOGMA_AXIOM_OBSERVER_PATH": "observer_path",
	} {
		if value := strings.TrimSpace(os.Getenv(env)); value != "" {
			static[field] = value
		}
	}

	return New(Config{
		Enabled:     enabled,
		Token:       token,
		Dataset:     envDefault("LOGMA_AXIOM_DATASET", DefaultDataset),
		Domain:      envDefault("LOGMA_AXIOM_DOMAIN", "api.axiom.co"),
		PublishMode: mode,
		Static:      static,
	})
}

func envDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
