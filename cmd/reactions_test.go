package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/mr-karan/calert/internal/notifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chatAPI is a minimal Google Chat double: an incoming webhook, an OAuth
// token endpoint and the reactions API of a single space.
type chatAPI struct {
	*httptest.Server

	mu        sync.Mutex
	requests  []string                     // "METHOD path", in arrival order
	withAuth  int                          // requests carrying an Authorization header
	messages  []string                     // message names, in creation order
	reactions map[string]map[string]string // message name -> emoji -> reaction name
}

func newChatAPI(t *testing.T) *chatAPI {
	t.Helper()
	api := &chatAPI{reactions: make(map[string]map[string]string)}
	api.Server = httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.Close)
	return api
}

func (api *chatAPI) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	api.mu.Lock()
	defer api.mu.Unlock()

	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	api.requests = append(api.requests, r.Method+" "+path)
	if r.Header.Get("Authorization") != "" {
		api.withAuth++
	}

	switch {
	case r.URL.Path == "/token":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`))
	case path == "spaces/AAAA/messages":
		name := fmt.Sprintf("spaces/AAAA/messages/m%d", len(api.messages)+1)
		api.messages = append(api.messages, name)
		api.reactions[name] = make(map[string]string)
		_, _ = fmt.Fprintf(w, `{"name":%q}`, name)
	case r.Header.Get("Authorization") != "Bearer test-token":
		w.WriteHeader(http.StatusUnauthorized)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/reactions"):
		var req struct {
			Emoji struct {
				Unicode string `json:"unicode"`
			} `json:"emoji"`
		}
		_ = json.Unmarshal(body, &req)
		message := strings.TrimSuffix(path, "/reactions")
		name := fmt.Sprintf("%s/reactions/r%d", message, len(api.requests))
		api.reactions[message][req.Emoji.Unicode] = name
		_, _ = fmt.Fprintf(w, `{"name":%q}`, name)
	case r.Method == http.MethodDelete:
		for _, onMessage := range api.reactions {
			for emoji, name := range onMessage {
				if name == path {
					delete(onMessage, emoji)
				}
			}
		}
		_, _ = w.Write([]byte(`{}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// emojis returns the sorted reactions on the nth message.
func (api *chatAPI) emojis(t *testing.T, n int) []string {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Greater(t, len(api.messages), n)
	out := make([]string, 0)
	for emoji := range api.reactions[api.messages[n]] {
		out = append(out, emoji)
	}
	sort.Strings(out)
	return out
}

// newConfiguredApp builds the app from a TOML config exactly as main does.
func newConfiguredApp(t *testing.T, config string) *App {
	t.Helper()
	ko := koanf.New(".")
	require.NoError(t, ko.Load(rawbytes.Provider([]byte(config)), toml.Parser()))

	lo := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mx := initMetrics()
	provs, err := initProviders(ko, lo, mx)
	require.NoError(t, err)
	n, err := notifier.Init(notifier.Opts{Providers: provs, Log: lo})
	require.NoError(t, err)
	return &App{lo: lo, metrics: mx, notifier: n, dispatchTimeout: 10 * time.Second}
}

// dispatch posts an Alertmanager webhook for a single alert to /dispatch.
func dispatch(t *testing.T, app *App, status string) {
	t.Helper()
	payload := fmt.Sprintf(`{
		"version": "4",
		"receiver": "prod_alerts",
		"status": %q,
		"groupKey": "{}:{alertname=\"HighCPU\"}",
		"alerts": [{
			"status": %q,
			"labels": {"alertname": "HighCPU", "severity": "critical"},
			"annotations": {"summary": "CPU is high"},
			"startsAt": "2026-06-12T09:00:00Z",
			"fingerprint": "c1ff1f9a5f6d2a4e"
		}]
	}`, status, status)
	req := httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	handleDispatchNotif(w, withAppContext(app, req))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

func TestDispatchUpdatesReactionsOnOriginalMessage(t *testing.T) {
	api := newChatAPI(t)
	// Emoji and remove_previous are left to their defaults on purpose.
	app := newConfiguredApp(t, fmt.Sprintf(`
[providers.prod_alerts]
type = "google_chat"
endpoint = "%s/v1/spaces/AAAA/messages?key=key&token=token"
template = "../static/message.tmpl"
threaded_replies = true
retry_max = 0

[providers.prod_alerts.reactions]
enabled = true
credentials_json = '{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh","token_uri":"%s/token"}'
`, api.URL, api.URL))

	dispatch(t, app, "firing")
	assert.Equal(t, []string{"🔥"}, api.emojis(t, 0))

	dispatch(t, app, "resolved")
	assert.Equal(t, []string{"✅"}, api.emojis(t, 0), "the original message must end up marked as resolved only")
	assert.Empty(t, api.emojis(t, 1))
}

func TestWebhookOnlyConfigNeverUsesAuthenticatedAPI(t *testing.T) {
	api := newChatAPI(t)
	app := newConfiguredApp(t, fmt.Sprintf(`
[providers.prod_alerts]
type = "google_chat"
endpoint = "%s/v1/spaces/AAAA/messages?key=key&token=token"
template = "../static/message.tmpl"
threaded_replies = true
`, api.URL))

	dispatch(t, app, "firing")
	dispatch(t, app, "resolved")

	api.mu.Lock()
	defer api.mu.Unlock()
	assert.Equal(t, []string{"POST spaces/AAAA/messages", "POST spaces/AAAA/messages"}, api.requests)
	assert.Zero(t, api.withAuth)
}

func TestReactionsRequireCredentialsAtStartup(t *testing.T) {
	ko := koanf.New(".")
	require.NoError(t, ko.Load(rawbytes.Provider([]byte(`
[providers.prod_alerts]
endpoint = "https://chat.googleapis.com/v1/spaces/AAAA/messages?key=key&token=token"
template = "../static/message.tmpl"

[providers.prod_alerts.reactions]
enabled = true
`)), toml.Parser()))

	_, err := initProviders(ko, slog.New(slog.NewJSONHandler(io.Discard, nil)), initMetrics())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reactions.credentials_file")
}

func TestSampleConfigLoadsWithReactionsDisabled(t *testing.T) {
	// main falls back to config.sample.toml, so it has to stay loadable.
	t.Chdir("..")
	ko := koanf.New(".")
	require.NoError(t, ko.Load(file.Provider("config.sample.toml"), toml.Parser()))

	provs, err := initProviders(ko, slog.New(slog.NewJSONHandler(io.Discard, nil)), initMetrics())

	require.NoError(t, err)
	assert.Len(t, provs, 2)
	for _, room := range ko.MapKeys("providers") {
		assert.False(t, ko.Bool("providers."+room+".reactions.enabled"), "reactions must stay opt-in")
	}
}
