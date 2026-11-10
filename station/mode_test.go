package main

import "testing"

func TestCodexModeSelection(t *testing.T) {
    cases := []struct {
        value string
        expected string
        accepted bool
    }{
        {"alice", "alice", true},
        {"stock", "alice", true},
        {"quasar", "quasar", true},
        {"custom", "quasar", true},
        {"gemini", "quasar", true}, // retired legacy selector MUST NOT start Codex
        {"codex", "codex", true},
        {"GPT Live", "codex", true},
        {"gpt-live-1-codex", "codex", true},
        {"GPT Realtime", "alice", false}, // experimental model MUST NOT be auto-selected
        {"not-a-mode", "alice", false},
    }
    for _, test := range cases {
        got, accepted := normalizeAssistantMode(test.value)
        if got != test.expected || accepted != test.accepted {
            t.Errorf("mode %q: got (%q,%v), want (%q,%v)", test.value, got, accepted, test.expected, test.accepted)
        }
    }
}

func TestStationEndpointsConfigurable(t *testing.T) {
    const key = "STATION_TEST_MODE_URL"
    t.Setenv(key, "http://127.0.0.1:2222/example")
    if got := envOrDefault(key, ""); got != "http://127.0.0.1:2222/example" {
        t.Errorf("unexpected configured endpoint: %s", got)
    }
}
