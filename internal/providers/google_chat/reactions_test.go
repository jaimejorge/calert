package google_chat

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mr-karan/calert/internal/metrics"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fakeAccessToken = "access-token-sentinel"
	fakeSpace       = "spaces/AAAA"
)

// fakeChat emulates the parts of Google Chat that calert talks to: the
// incoming webhook, the OAuth token endpoint and the reactions API.
type fakeChat struct {
	*httptest.Server

	mu            sync.Mutex
	messages      []string                     // message names, in creation order
	threadKeys    map[string]string            // message name -> threadKey
	reactions     map[string]map[string]string // message name -> emoji -> reaction name
	calls         []string                     // every request, in arrival order
	tokenRequests int
	tokenClaims   map[string]any // claims of the last service account assertion
	nextID        int

	// tokenStatus, when set, makes the token endpoint reject requests.
	tokenStatus int
	// webhookBody, when set, replaces the webhook response body.
	webhookBody *string
	// failWebhookAt makes the Nth webhook request (1-based) return a 400.
	failWebhookAt int
	// onReaction runs before a reactions API request is served. A non-zero
	// return value is sent as the response status instead.
	onReaction func(method, path string) int
}

func newFakeChat(t *testing.T) *fakeChat {
	t.Helper()
	fc := &fakeChat{
		threadKeys: make(map[string]string),
		reactions:  make(map[string]map[string]string),
	}
	fc.Server = httptest.NewServer(http.HandlerFunc(fc.serve))
	t.Cleanup(fc.Close)
	return fc
}

func (fc *fakeChat) endpoint() string {
	return fc.URL + "/v1/" + fakeSpace + "/messages?key=webhook-key-sentinel&token=webhook-token-sentinel"
}

func (fc *fakeChat) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	path := strings.TrimPrefix(r.URL.Path, "/v1/")

	switch {
	case r.URL.Path == "/token":
		fc.serveToken(w, r, body)
	case path == fakeSpace+"/messages" && r.Method == http.MethodPost:
		fc.serveWebhook(w, r)
	case strings.Contains(path, "/reactions"):
		fc.serveReaction(w, r, path, body)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (fc *fakeChat) serveToken(w http.ResponseWriter, r *http.Request, body []byte) {
	fc.mu.Lock()
	fc.tokenRequests++
	fc.calls = append(fc.calls, "TOKEN")
	status := fc.tokenStatus
	if form, err := parseForm(body); err == nil && form["assertion"] != "" {
		fc.tokenClaims = decodeJWTClaims(form["assertion"])
	}
	fc.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if status != 0 {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"token-description-sentinel"}`))
		return
	}
	_, _ = fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":3600}`, fakeAccessToken)
}

func (fc *fakeChat) serveWebhook(w http.ResponseWriter, r *http.Request) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.calls = append(fc.calls, "MESSAGE")
	if fc.failWebhookAt > 0 && countOf(fc.calls, "MESSAGE") == fc.failWebhookAt {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fc.nextID++
	name := fmt.Sprintf("%s/messages/msg%d.msg%d", fakeSpace, fc.nextID, fc.nextID)
	fc.messages = append(fc.messages, name)
	fc.threadKeys[name] = r.URL.Query().Get("threadKey")
	fc.reactions[name] = make(map[string]string)

	if fc.webhookBody != nil {
		_, _ = w.Write([]byte(*fc.webhookBody))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "thread": map[string]string{"name": fakeSpace + "/threads/t"}})
}

func (fc *fakeChat) serveReaction(w http.ResponseWriter, r *http.Request, path string, body []byte) {
	fc.mu.Lock()
	hook := fc.onReaction
	fc.mu.Unlock()
	// The hook runs unlocked so that it can call back into the fake.
	var forced int
	if hook != nil {
		forced = hook(r.Method, path)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+fakeAccessToken {
		fc.calls = append(fc.calls, "UNAUTHENTICATED "+path)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodPost:
		message := strings.TrimSuffix(path, "/reactions")
		var req struct {
			Emoji struct {
				Unicode string `json:"unicode"`
			} `json:"emoji"`
		}
		_ = json.Unmarshal(body, &req)
		fc.calls = append(fc.calls, "ADD "+req.Emoji.Unicode+" "+message)
		if forced != 0 {
			w.WriteHeader(forced)
			_, _ = w.Write([]byte(`{"error":{"message":"api-body-sentinel"}}`))
			return
		}
		existing, ok := fc.reactions[message]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if _, dup := existing[req.Emoji.Unicode]; dup {
			w.WriteHeader(http.StatusConflict)
			return
		}
		fc.nextID++
		name := fmt.Sprintf("%s/reactions/r%d", message, fc.nextID)
		existing[req.Emoji.Unicode] = name
		_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "emoji": map[string]string{"unicode": req.Emoji.Unicode}})
	case http.MethodDelete:
		fc.calls = append(fc.calls, "DELETE "+path)
		if forced != 0 {
			w.WriteHeader(forced)
			return
		}
		for _, existing := range fc.reactions {
			for emoji, name := range existing {
				if name == path {
					delete(existing, emoji)
					_, _ = w.Write([]byte(`{}`))
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// message returns the name of the nth message posted through the webhook.
func (fc *fakeChat) message(t *testing.T, n int) string {
	t.Helper()
	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Greater(t, len(fc.messages), n, "expected at least %d messages", n+1)
	return fc.messages[n]
}

func (fc *fakeChat) messageCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.messages)
}

func (fc *fakeChat) threadKey(message string) string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.threadKeys[message]
}

// emojis returns the sorted reactions currently on message.
func (fc *fakeChat) emojis(message string) []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	out := make([]string, 0, len(fc.reactions[message]))
	for emoji := range fc.reactions[message] {
		out = append(out, emoji)
	}
	sort.Strings(out)
	return out
}

// deleteMessage emulates a user deleting a message, reactions included.
func (fc *fakeChat) deleteMessage(message string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	delete(fc.reactions, message)
}

// reactionCalls returns the reactions API requests seen so far.
func (fc *fakeChat) reactionCalls() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	out := make([]string, 0)
	for _, call := range fc.calls {
		if call != "MESSAGE" && call != "TOKEN" {
			out = append(out, call)
		}
	}
	return out
}

func (fc *fakeChat) allCalls() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]string(nil), fc.calls...)
}

func (fc *fakeChat) set(change func(*fakeChat)) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	change(fc)
}

func countOf(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}

func parseForm(body []byte) (map[string]string, error) {
	req, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := req.ParseForm(); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(req.PostForm))
	for key := range req.PostForm {
		out[key] = req.PostForm.Get(key)
	}
	return out, nil
}

func decodeJWTClaims(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	_ = json.Unmarshal(payload, &claims)
	return claims
}

// userCredentials returns `authorized_user` credentials whose refresh token
// is exchanged at the fake's token endpoint.
func (fc *fakeChat) userCredentials() string {
	return fmt.Sprintf(`{"type":"authorized_user","client_id":"client-id","client_secret":"client-secret-sentinel","refresh_token":"refresh-token-sentinel","token_uri":%q}`, fc.URL+"/token")
}

func reactionBackends() []string {
	return []string{"memory", "redis"}
}

// newReactionChat builds an alert-mode provider with reactions enabled
// against fc. backend selects the reaction store: "memory", "redis" (a fresh
// miniredis) or an explicit Redis address.
func newReactionChat(t *testing.T, fc *fakeChat, backend string, mutate ...func(*GoogleChatOpts)) *GoogleChatManager {
	t.Helper()
	opts := GoogleChatOpts{
		Log:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics:         metrics.New("calert"),
		Endpoint:        fc.endpoint(),
		Room:            "room",
		Template:        "../../../static/message.tmpl",
		ThreadedReplies: true,
		Timeout:         5 * time.Second,
		RetryMax:        1,
		RetryWaitMin:    time.Millisecond,
		RetryWaitMax:    2 * time.Millisecond,
		Reactions: ReactionOpts{
			Enabled:         true,
			Firing:          "🔥",
			Resolved:        "✅",
			RemovePrevious:  true,
			CredentialsJSON: fc.userCredentials(),
		},
	}
	switch backend {
	case "memory":
	case "redis":
		opts.RedisAddress = miniredis.RunT(t).Addr()
	default:
		opts.RedisAddress = backend
	}
	for _, m := range mutate {
		m(&opts)
	}
	chat, err := NewGoogleChat(opts)
	require.NoError(t, err)
	return chat
}

func metricsOutput(chat *GoogleChatManager) string {
	var out bytes.Buffer
	chat.metrics.FlushMetrics(&out)
	return out.String()
}

func resolvedCopy(alert alertmgrtmpl.Alert) alertmgrtmpl.Alert {
	alert.Status = "resolved"
	alert.EndsAt = alert.StartsAt.Add(5 * time.Minute)
	return alert
}

func TestReactionsFollowAlertLifecycle(t *testing.T) {
	for _, backend := range reactionBackends() {
		t.Run(backend, func(t *testing.T) {
			fc := newFakeChat(t)
			chat := newReactionChat(t, fc, backend)
			ctx := context.Background()
			firing := groupAlert("fp-a", "firing", "node1")

			require.NoError(t, chat.Push(ctx, groupPayload(firing)))

			original := fc.message(t, 0)
			assert.Equal(t, []string{"🔥"}, fc.emojis(original), "FIRING must mark the original message")
			key := threadKeyForAlert(chat.room, firing.Fingerprint, firing.StartsAt)
			record, ok, err := chat.reactions.store.get(ctx, key)
			require.NoError(t, err)
			require.True(t, ok, "FIRING must store the original message reference")
			assert.Equal(t, original, record.Message)
			assert.NotEmpty(t, record.FiringReaction)
			assert.False(t, record.Resolved)

			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

			require.Equal(t, 2, fc.messageCount(), "the RESOLVED notification must still be delivered")
			assert.Equal(t, []string{"✅"}, fc.emojis(original), "RESOLVED must swap the reaction on the original message")
			assert.Empty(t, fc.emojis(fc.message(t, 1)), "the RESOLVED notification itself gets no reaction")
			assert.Equal(t, fc.threadKey(original), fc.threadKey(fc.message(t, 1)), "both notifications must share a thread")
			assert.Equal(t, key, fc.threadKey(original))

			record, ok, err = chat.reactions.store.get(ctx, key)
			require.NoError(t, err)
			require.True(t, ok)
			assert.True(t, record.Resolved)

			out := metricsOutput(chat)
			assert.Contains(t, out, `calert_reactions_total{provider="google_chat", room="room", action="add"} 2`)
			assert.Contains(t, out, `calert_reactions_total{provider="google_chat", room="room", action="remove"} 1`)
			assert.NotContains(t, out, "reactions_errors_total")
		})
	}
}

func TestReactionsAreIdempotentForDuplicateEvents(t *testing.T) {
	for _, backend := range reactionBackends() {
		t.Run(backend, func(t *testing.T) {
			fc := newFakeChat(t)
			chat := newReactionChat(t, fc, backend)
			ctx := context.Background()
			firing := groupAlert("fp-a", "firing", "node1")

			// Alertmanager repeats and retries deliver the same FIRING alert again.
			require.NoError(t, chat.Push(ctx, groupPayload(firing)))
			require.NoError(t, chat.Push(ctx, groupPayload(firing)))

			original := fc.message(t, 0)
			require.Equal(t, 2, fc.messageCount(), "duplicate notifications keep their existing behaviour")
			assert.Equal(t, []string{"🔥"}, fc.emojis(original))
			assert.Empty(t, fc.emojis(fc.message(t, 1)), "only the original message carries the reaction")
			assert.Equal(t, []string{"ADD 🔥 " + original}, fc.reactionCalls())

			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))
			calls := fc.reactionCalls()
			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

			require.Equal(t, 4, fc.messageCount())
			assert.Equal(t, []string{"✅"}, fc.emojis(original))
			assert.Equal(t, calls, fc.reactionCalls(), "a duplicate RESOLVED must not touch reactions again")
			assert.Len(t, calls, 3)

			// A late FIRING duplicate must not bring the firing reaction back.
			require.NoError(t, chat.Push(ctx, groupPayload(firing)))
			assert.Equal(t, []string{"✅"}, fc.emojis(original))
			assert.Equal(t, calls, fc.reactionCalls())
		})
	}
}

func TestLateFiringDuplicateDoesNotMarkResolvedAlertAsFiring(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory")
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	// The firing reaction could not be added, so none is on record.
	fc.set(func(fc *fakeChat) { fc.onReaction = func(string, string) int { return http.StatusInternalServerError } })
	require.NoError(t, chat.Push(ctx, groupPayload(firing)))
	fc.set(func(fc *fakeChat) { fc.onReaction = nil })
	original := fc.message(t, 0)
	require.Empty(t, fc.emojis(original))

	require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))
	require.Equal(t, []string{"✅"}, fc.emojis(original))

	calls := fc.reactionCalls()
	require.NoError(t, chat.Push(ctx, groupPayload(firing)))
	assert.Equal(t, []string{"✅"}, fc.emojis(original))
	assert.Equal(t, calls, fc.reactionCalls(), "a FIRING retry arriving after RESOLVED must not add the firing reaction, not even briefly")
}

func TestReactionsTrackAlertsIndependently(t *testing.T) {
	for _, backend := range reactionBackends() {
		t.Run(backend, func(t *testing.T) {
			fc := newFakeChat(t)
			chat := newReactionChat(t, fc, backend)
			ctx := context.Background()
			alertA := groupAlert("fp-a", "firing", "node1")
			alertB := groupAlert("fp-b", "firing", "node2")

			require.NoError(t, chat.Push(ctx, groupPayload(alertA, alertB)))
			messageA, messageB := fc.message(t, 0), fc.message(t, 1)
			assert.Equal(t, []string{"🔥"}, fc.emojis(messageA))
			assert.Equal(t, []string{"🔥"}, fc.emojis(messageB))

			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(alertB))))
			assert.Equal(t, []string{"🔥"}, fc.emojis(messageA), "an unrelated alert must keep its firing reaction")
			assert.Equal(t, []string{"✅"}, fc.emojis(messageB))

			// A refire has a new startsAt and therefore a new original message.
			refire := alertB
			refire.StartsAt = alertB.StartsAt.Add(time.Hour)
			require.NoError(t, chat.Push(ctx, groupPayload(refire)))
			messageRefire := fc.message(t, 3)
			assert.Equal(t, []string{"🔥"}, fc.emojis(messageRefire))
			assert.Equal(t, []string{"✅"}, fc.emojis(messageB), "the previous lifecycle stays resolved")

			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(refire))))
			assert.Equal(t, []string{"✅"}, fc.emojis(messageRefire))
			assert.Equal(t, []string{"🔥"}, fc.emojis(messageA))
		})
	}
}

func TestResolvedWithoutOriginalMessageIsStillDelivered(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory")

	require.NoError(t, chat.Push(context.Background(), groupPayload(resolvedCopy(groupAlert("fp-a", "firing", "node1")))))

	assert.Equal(t, 1, fc.messageCount())
	assert.Equal(t, []string{"MESSAGE"}, fc.allCalls(), "no token or reaction request without a known original message")
	out := metricsOutput(chat)
	assert.Contains(t, out, `calert_reactions_skipped_total{provider="google_chat", room="room", reason="missing_reference"} 1`)
	assert.NotContains(t, out, "reactions_errors_total")
}

func TestReactionFailuresNeverFailDelivery(t *testing.T) {
	cases := []struct {
		name   string
		status int
		reason string
	}{
		{"server error", http.StatusInternalServerError, "api"},
		{"permission denied", http.StatusForbidden, "auth"},
		{"rate limited", http.StatusTooManyRequests, "rate_limited"},
		{"bad request", http.StatusBadRequest, "api"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			fc := newFakeChat(t)
			chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) {
				o.Log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			})
			ctx := context.Background()
			firing := groupAlert("fp-a", "firing", "node1")

			// The firing reaction is in place before the API starts failing.
			require.NoError(t, chat.Push(ctx, groupPayload(firing)))
			original := fc.message(t, 0)
			require.Equal(t, []string{"🔥"}, fc.emojis(original))

			fc.set(func(fc *fakeChat) { fc.onReaction = func(string, string) int { return tc.status } })
			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))), "a reaction failure must not fail the dispatch")
			require.Equal(t, 2, fc.messageCount(), "the RESOLVED notification must be delivered")
			assert.Equal(t, []string{"🔥"}, fc.emojis(original))

			out := metricsOutput(chat)
			assert.Contains(t, out, fmt.Sprintf(`calert_reactions_errors_total{provider="google_chat", room="room", action="remove", reason="%s"} 1`, tc.reason))
			assert.Contains(t, out, fmt.Sprintf(`calert_reactions_errors_total{provider="google_chat", room="room", action="add", reason="%s"} 1`, tc.reason))
			assert.Contains(t, out, `calert_alerts_dispatched_total{provider="google_chat", room="room"} 2`)
			assert.NotContains(t, out, "alerts_dispatched_errors_total")

			// Once the API recovers, a retried RESOLVED finishes the update.
			fc.set(func(fc *fakeChat) { fc.onReaction = nil })
			require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))
			assert.Equal(t, []string{"✅"}, fc.emojis(original))

			for _, secret := range []string{"sentinel", fakeAccessToken} {
				assert.NotContains(t, logs.String(), secret)
			}
		})
	}
}

func TestReactionRetriesTransientFailures(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.RetryMax = 2 })
	var attempts int
	fc.set(func(fc *fakeChat) {
		fc.onReaction = func(string, string) int {
			attempts++
			if attempts == 1 {
				return http.StatusTooManyRequests
			}
			return 0
		}
	})

	require.NoError(t, chat.Push(context.Background(), groupPayload(groupAlert("fp-a", "firing", "node1"))))

	assert.Equal(t, []string{"🔥"}, fc.emojis(fc.message(t, 0)), "a rate limited request must be retried")
	assert.Equal(t, 2, attempts)
	assert.NotContains(t, metricsOutput(chat), "reactions_errors_total")
}

func TestReactionAuthenticationFailureIsNotRetriedAndDoesNotLeak(t *testing.T) {
	var logs bytes.Buffer
	fc := newFakeChat(t)
	fc.tokenStatus = http.StatusBadRequest
	chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) {
		o.Log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		o.RetryMax = 3
	})

	require.NoError(t, chat.Push(context.Background(), groupPayload(groupAlert("fp-a", "firing", "node1"))))

	assert.Equal(t, 1, fc.messageCount(), "the notification must be delivered without working credentials")
	assert.Equal(t, []string{"MESSAGE", "TOKEN"}, fc.allCalls(), "a rejected token request must not be retried")
	assert.Contains(t, metricsOutput(chat), `calert_reactions_errors_total{provider="google_chat", room="room", action="add", reason="auth"} 1`)
	assert.Contains(t, logs.String(), "invalid_grant")
	assert.NotContains(t, logs.String(), "sentinel")
}

func TestReactionStoreOutageSkipsReactionsButDelivers(t *testing.T) {
	fc := newFakeChat(t)
	// Nothing listens on this address, so every store call fails.
	chat := newReactionChat(t, fc, "127.0.0.1:1")
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	require.NoError(t, chat.Push(ctx, groupPayload(firing)))
	require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

	assert.Equal(t, 2, fc.messageCount())
	assert.Empty(t, fc.reactionCalls(), "a reaction that cannot be tracked must not be added")
	assert.Contains(t, metricsOutput(chat), `calert_reactions_errors_total{provider="google_chat", room="room", action="store", reason="store"} 2`)
}

func TestReactionsRunAfterEveryNotificationIsAttempted(t *testing.T) {
	t.Run("notifications are sent before any reaction", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory")

		require.NoError(t, chat.Push(context.Background(), groupPayload(
			groupAlert("fp-a", "firing", "node1"),
			groupAlert("fp-b", "firing", "node2"),
		)))

		calls := fc.allCalls()
		require.GreaterOrEqual(t, len(calls), 2)
		assert.Equal(t, []string{"MESSAGE", "MESSAGE"}, calls[:2])
	})

	t.Run("a failed notification keeps the reactions of delivered ones", func(t *testing.T) {
		fc := newFakeChat(t)
		fc.failWebhookAt = 2
		chat := newReactionChat(t, fc, "memory")

		err := chat.Push(context.Background(), groupPayload(
			groupAlert("fp-a", "firing", "node1"),
			groupAlert("fp-b", "firing", "node2"),
		))

		require.Error(t, err, "webhook failures keep failing the dispatch")
		require.Equal(t, 1, fc.messageCount())
		assert.Equal(t, []string{"🔥"}, fc.emojis(fc.message(t, 0)))
	})
}

func TestReactionOptions(t *testing.T) {
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	t.Run("remove_previous disabled keeps the firing reaction", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.Reactions.RemovePrevious = false })

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))
		require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

		assert.Equal(t, []string{"✅", "🔥"}, fc.emojis(fc.message(t, 0)))
	})

	t.Run("empty firing emoji only marks resolved alerts", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.Reactions.Firing = "" })

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))
		assert.Empty(t, fc.reactionCalls())
		require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

		assert.Equal(t, []string{"✅"}, fc.emojis(fc.message(t, 0)))
	})

	t.Run("empty resolved emoji only clears the firing reaction", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.Reactions.Resolved = "" })

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))
		require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

		assert.Empty(t, fc.emojis(fc.message(t, 0)))
	})

	t.Run("unthreaded rooms still mark the original message", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.ThreadedReplies = false })

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))
		require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

		assert.Equal(t, []string{"✅"}, fc.emojis(fc.message(t, 0)))
		assert.Empty(t, fc.threadKey(fc.message(t, 0)))
	})

	t.Run("dry run sends neither notifications nor reactions", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.DryRun = true })

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))

		assert.Empty(t, fc.allCalls())
	})
}

func TestReactionsDisabledLeavesWebhookBehaviourUnchanged(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) { o.Reactions = ReactionOpts{} })
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	require.Nil(t, chat.reactions)
	require.NoError(t, chat.Push(ctx, groupPayload(firing)))
	require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

	assert.Equal(t, []string{"MESSAGE", "MESSAGE"}, fc.allCalls(), "a webhook-only room must not call any other API")
	assert.NotContains(t, metricsOutput(chat), "reactions_")
}

func TestReactionsTolerateWebhookResponsesWithoutMessageName(t *testing.T) {
	for _, body := range []string{``, `{}`, `not json`, `{"name":"spaces/AAAA/messages/../../other"}`} {
		t.Run(body, func(t *testing.T) {
			fc := newFakeChat(t)
			fc.webhookBody = &body
			chat := newReactionChat(t, fc, "memory")

			require.NoError(t, chat.Push(context.Background(), groupPayload(groupAlert("fp-a", "firing", "node1"))))

			assert.Equal(t, []string{"MESSAGE"}, fc.allCalls())
			assert.Contains(t, metricsOutput(chat), `calert_reactions_skipped_total{provider="google_chat", room="room", reason="missing_message_name"} 1`)
		})
	}
}

func TestReactionsHandleDeletedOriginalMessage(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory")
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	require.NoError(t, chat.Push(ctx, groupPayload(firing)))
	fc.deleteMessage(fc.message(t, 0))
	require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

	assert.Equal(t, 2, fc.messageCount())
	assert.NotContains(t, metricsOutput(chat), "reactions_errors_total", "a deleted message is not an error")
	record, ok, err := chat.reactions.store.get(ctx, threadKeyForAlert(chat.room, firing.Fingerprint, firing.StartsAt))
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, record.Resolved, "the lifecycle is finished and must not be retried")
}

func TestReactionsAreSharedAcrossReplicasThroughRedis(t *testing.T) {
	fc := newFakeChat(t)
	redisAddr := miniredis.RunT(t).Addr()
	replicaA := newReactionChat(t, fc, redisAddr)
	replicaB := newReactionChat(t, fc, redisAddr)
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	// Both replicas receive the FIRING alert, another one the RESOLVED alert.
	require.NoError(t, replicaA.Push(ctx, groupPayload(firing)))
	require.NoError(t, replicaB.Push(ctx, groupPayload(firing)))
	original := fc.message(t, 0)
	assert.Equal(t, []string{"🔥"}, fc.emojis(original))
	assert.Empty(t, fc.emojis(fc.message(t, 1)), "replicas must agree on a single original message")

	require.NoError(t, replicaB.Push(ctx, groupPayload(resolvedCopy(firing))))
	assert.Equal(t, []string{"✅"}, fc.emojis(original))

	require.NoError(t, replicaA.Push(ctx, groupPayload(resolvedCopy(firing))))
	assert.Equal(t, []string{"✅"}, fc.emojis(original))
	assert.NotContains(t, metricsOutput(replicaA), "reactions_errors_total")
	assert.NotContains(t, metricsOutput(replicaB), "reactions_errors_total")
}

func TestConcurrentReplicasConvergeOnTheSameReactions(t *testing.T) {
	fc := newFakeChat(t)
	redisAddr := miniredis.RunT(t).Addr()
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	const replicas = 4
	chats := make([]*GoogleChatManager, replicas)
	for i := range chats {
		chats[i] = newReactionChat(t, fc, redisAddr)
	}
	pushAll := func(alert alertmgrtmpl.Alert) {
		var wg sync.WaitGroup
		for _, chat := range chats {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, chat.Push(ctx, groupPayload(alert)))
			}()
		}
		wg.Wait()
	}

	pushAll(firing)
	record, ok, err := chats[0].reactions.store.get(ctx, threadKeyForAlert("room", firing.Fingerprint, firing.StartsAt))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []string{"🔥"}, fc.emojis(record.Message))

	pushAll(resolvedCopy(firing))
	assert.Equal(t, []string{"✅"}, fc.emojis(record.Message))
	for i := 0; i < fc.messageCount(); i++ {
		if message := fc.message(t, i); message != record.Message {
			assert.Empty(t, fc.emojis(message))
		}
	}
}

func TestReactionRacesBetweenFiringAndResolved(t *testing.T) {
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	t.Run("alert resolves while the firing reaction is being added", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory")
		key := threadKeyForAlert(chat.room, firing.Fingerprint, firing.StartsAt)
		fc.set(func(fc *fakeChat) {
			fc.onReaction = func(method, _ string) int {
				if method == http.MethodPost {
					_, err := chat.reactions.store.markResolved(ctx, key)
					assert.NoError(t, err)
				}
				return 0
			}
		})

		require.NoError(t, chat.Push(ctx, groupPayload(firing)))

		assert.Empty(t, fc.emojis(fc.message(t, 0)), "a firing reaction added after the alert resolved must be removed again")
	})

	t.Run("firing reaction is recorded while the alert is being resolved", func(t *testing.T) {
		fc := newFakeChat(t)
		chat := newReactionChat(t, fc, "memory")
		key := threadKeyForAlert(chat.room, firing.Fingerprint, firing.StartsAt)

		// The original message is known, but its firing reaction is not yet.
		require.NoError(t, chat.Push(ctx, groupPayload(groupAlert("fp-other", "firing", "node2"))))
		original := fc.message(t, 0)
		_, err := chat.reactions.store.claim(ctx, key, original)
		require.NoError(t, err)
		late := fc.reactions[original]["🔥"]
		require.NotEmpty(t, late)

		fc.set(func(fc *fakeChat) {
			fc.onReaction = func(method, _ string) int {
				if method == http.MethodPost {
					_, err := chat.reactions.store.setFiringReaction(ctx, key, late)
					assert.NoError(t, err)
				}
				return 0
			}
		})
		chat.reactions.onResolved(ctx, key)

		assert.Equal(t, []string{"✅"}, fc.emojis(original), "a firing reaction recorded mid-resolve must still be removed")
	})
}

func TestReactionsAreIgnoredInGroupMode(t *testing.T) {
	var logs bytes.Buffer
	srv := newCapturingServer()
	defer srv.Close()

	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:             slog.New(slog.NewJSONHandler(&logs, nil)),
		Metrics:         metrics.New("calert"),
		Endpoint:        srv.URL,
		Room:            "room",
		Template:        "../../../static/message-group.tmpl",
		ThreadingMode:   ThreadingModeGroup,
		MaxAlertsPerMsg: 10,
		// No credentials: group mode must not even try to load them.
		Reactions: ReactionOpts{Enabled: true, Firing: "🔥", Resolved: "✅"},
	})
	require.NoError(t, err)

	assert.Nil(t, chat.reactions)
	assert.Contains(t, logs.String(), "reactions are ignored in group threading mode")
	require.NoError(t, chat.Push(context.Background(), groupPayload(groupAlert("fp-a", "firing", "node1"))))
	assert.Len(t, srv.keys(), 1)
}

func TestReactionConfigurationIsValidated(t *testing.T) {
	credentialsFile := filepath.Join(t.TempDir(), "credentials.json")
	user := `{"type":"authorized_user","client_id":"id","client_secret":"client-secret-sentinel","refresh_token":"refresh-token-sentinel"}`
	require.NoError(t, os.WriteFile(credentialsFile, []byte(user), 0o600))

	cases := []struct {
		name    string
		mutate  func(*GoogleChatOpts)
		wantErr string
	}{
		{"credentials file", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = ""
			o.Reactions.CredentialsFile = credentialsFile
		}, ""},
		{"credentials json", func(o *GoogleChatOpts) { o.Reactions.CredentialsJSON = user }, ""},
		{"no credentials", func(o *GoogleChatOpts) { o.Reactions.CredentialsJSON = "" }, "an incoming webhook cannot manage reactions"},
		{"both credential sources", func(o *GoogleChatOpts) { o.Reactions.CredentialsFile = credentialsFile }, "only one of"},
		{"missing credentials file", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = ""
			o.Reactions.CredentialsFile = filepath.Join(t.TempDir(), "missing.json")
		}, "reading reactions.credentials_file"},
		{"malformed credentials", func(o *GoogleChatOpts) { o.Reactions.CredentialsJSON = `{"client_secret":"client-secret-sentinel"` }, "not valid JSON"},
		{"incomplete user credentials", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = `{"type":"authorized_user","client_secret":"client-secret-sentinel"}`
		}, "need client_id, client_secret and refresh_token"},
		{"unsupported credentials type", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = `{"type":"external_account","client_secret":"client-secret-sentinel"}`
		}, "unsupported reactions credentials type"},
		{"service account without user", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = `{"type":"service_account","private_key":"private-key-sentinel"}`
		}, "need reactions.impersonate_user"},
		{"invalid service account", func(o *GoogleChatOpts) {
			o.Reactions.CredentialsJSON = `{"type":"service_account","private_key":["private-key-sentinel"]}`
			o.Reactions.ImpersonateUser = "bot@example.com"
		}, "service_account credentials are invalid"},
		{"impersonation with user credentials", func(o *GoogleChatOpts) { o.Reactions.ImpersonateUser = "bot@example.com" }, "only applies to service_account"},
		{"no emoji at all", func(o *GoogleChatOpts) { o.Reactions.Firing, o.Reactions.Resolved = "", "" }, "must not both be empty"},
		{"negative ttl", func(o *GoogleChatOpts) { o.Reactions.TTL = -time.Second }, "reactions.ttl must not be negative"},
		{"relative endpoint", func(o *GoogleChatOpts) { o.Endpoint = "/v1/spaces/AAAA/messages" }, "absolute endpoint URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := GoogleChatOpts{
				Log:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
				Metrics:  metrics.New("calert"),
				Endpoint: "https://chat.example.test/v1/spaces/AAAA/messages?key=k&token=webhook-token-sentinel",
				Room:     "room",
				Template: "../../../static/message.tmpl",
				Reactions: ReactionOpts{
					Enabled:         true,
					Firing:          "🔥",
					Resolved:        "✅",
					CredentialsJSON: user,
				},
			}
			tc.mutate(&opts)

			chat, err := NewGoogleChat(opts)

			if tc.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, chat.reactions)
				assert.Equal(t, "https://chat.example.test/v1", chat.reactions.apiBase)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), "sentinel")
		})
	}
}

func TestReactionsWithImpersonatedServiceAccount(t *testing.T) {
	fc := newFakeChat(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	credentials, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"client_email":   "calert@project.iam.gserviceaccount.com",
		"private_key_id": "key-id",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri":      fc.URL + "/token",
	})
	require.NoError(t, err)

	chat := newReactionChat(t, fc, "memory", func(o *GoogleChatOpts) {
		o.Reactions.CredentialsJSON = string(credentials)
		o.Reactions.ImpersonateUser = "alerts-bot@example.com"
	})
	require.NoError(t, chat.Push(context.Background(), groupPayload(groupAlert("fp-a", "firing", "node1"))))

	assert.Equal(t, []string{"🔥"}, fc.emojis(fc.message(t, 0)))
	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.NotNil(t, fc.tokenClaims, "the service account must authenticate with a signed assertion")
	assert.Equal(t, "alerts-bot@example.com", fc.tokenClaims["sub"], "reactions must be made as the impersonated user")
	assert.Equal(t, "calert@project.iam.gserviceaccount.com", fc.tokenClaims["iss"])
	assert.Equal(t, reactionsScope, fc.tokenClaims["scope"])
}

func TestAccessTokenIsReusedAcrossReactions(t *testing.T) {
	fc := newFakeChat(t)
	chat := newReactionChat(t, fc, "memory")
	ctx := context.Background()
	firing := groupAlert("fp-a", "firing", "node1")

	require.NoError(t, chat.Push(ctx, groupPayload(firing, groupAlert("fp-b", "firing", "node2"))))
	require.NoError(t, chat.Push(ctx, groupPayload(resolvedCopy(firing))))

	assert.Equal(t, 1, countOf(fc.allCalls(), "TOKEN"))
}

func TestValidResourceName(t *testing.T) {
	message := []string{"spaces", "messages"}
	reaction := []string{"spaces", "messages", "reactions"}

	assert.True(t, validResourceName("spaces/AAAA/messages/BBBB.BBBB", message...))
	assert.True(t, validResourceName("spaces/AAAA/messages/client-custom_id", message...))
	assert.True(t, validResourceName("spaces/AAAA/messages/BBBB.BBBB/reactions/CCCC", reaction...))

	for _, name := range []string{
		"",
		"spaces/AAAA",
		"spaces/AAAA/messages/",
		"spaces//messages/BBBB",
		"rooms/AAAA/messages/BBBB",
		"spaces/AAAA/messages/BBBB/reactions/CCCC",
		"spaces/AAAA/messages/..",
		"spaces/./messages/BBBB",
		"spaces/AAAA/messages/BBBB?alt=media",
		"spaces/AAAA/messages/BBBB#fragment",
		"spaces/AAAA/messages/BB%2FBB",
		"spaces/AAAA/messages/BB BB",
		"/spaces/AAAA/messages/BBBB",
	} {
		assert.False(t, validResourceName(name, message...), name)
	}
	assert.False(t, validResourceName("spaces/AAAA/messages/BBBB", reaction...))
}
