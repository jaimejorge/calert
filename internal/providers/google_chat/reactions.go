package google_chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/mr-karan/calert/internal/metrics"
	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	chatv1 "google.golang.org/api/chat/v1"
)

const (
	// reactionsScope covers both spaces.messages.reactions.create and
	// spaces.messages.reactions.delete.
	reactionsScope = "https://www.googleapis.com/auth/chat.messages.reactions"

	defaultReactionTTL = 7 * 24 * time.Hour

	// maxChatResponseSize bounds how much of a Chat API response is read.
	maxChatResponseSize = 1 << 20
)

// ReactionOpts configures the optional reactions that mark the state of an
// alert on its original Google Chat message.
type ReactionOpts struct {
	Enabled bool
	// Firing and Resolved are the unicode emoji added to the original
	// message for each state. An empty value skips that reaction.
	Firing   string
	Resolved string
	// RemovePrevious removes the firing reaction once the alert resolves.
	RemovePrevious bool
	// CredentialsFile or CredentialsJSON hold Google credentials of type
	// `authorized_user` or `service_account`. Exactly one must be set.
	CredentialsFile string
	CredentialsJSON string
	// ImpersonateUser is the Workspace user a service account acts as through
	// domain-wide delegation. Google Chat only accepts reactions from users.
	ImpersonateUser string
	// TTL is how long the original message of an alert is remembered.
	TTL time.Duration
}

var (
	errReactionExists   = errors.New("reaction already exists")
	errReactionNotFound = errors.New("message or reaction not found")
)

// reactionError is a failed reaction update, reduced to what is safe to log
// and to a reason usable as a metric label.
type reactionError struct {
	reason string
	detail string
}

func (e *reactionError) Error() string { return e.detail }

// reactionEvent is one delivered alert notification whose reaction still has
// to be updated.
type reactionEvent struct {
	status  string
	key     string
	message string
}

// reactionManager updates reactions through the authenticated Google Chat
// API. Incoming webhooks can only create messages, so it uses its own client.
type reactionManager struct {
	lo             *slog.Logger
	metrics        *metrics.Manager
	provider       string
	room           string
	client         *retryablehttp.Client
	apiBase        string
	store          reactionStore
	firing         string
	resolved       string
	removePrevious bool
}

func newReactionManager(provider string, opts GoogleChatOpts, transport http.RoundTripper, checkRetry retryablehttp.CheckRetry) (*reactionManager, error) {
	ro := opts.Reactions
	if ro.Firing == "" && ro.Resolved == "" {
		return nil, fmt.Errorf("reactions.firing and reactions.resolved must not both be empty")
	}
	if ro.TTL < 0 {
		return nil, fmt.Errorf("reactions.ttl must not be negative")
	}
	ttl := ro.TTL
	if ttl == 0 {
		ttl = defaultReactionTTL
	}

	// Reactions live on the same API host as the webhook that posts messages.
	endpoint, err := url.Parse(opts.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("reactions require an absolute endpoint URL")
	}

	// Token requests use the same transport, and so the same proxy, as the
	// API calls themselves.
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport, Timeout: opts.Timeout})
	tokens, err := reactionTokenSource(ctx, ro)
	if err != nil {
		return nil, err
	}

	client := retryablehttp.NewClient()
	client.RetryMax = opts.RetryMax
	client.RetryWaitMin = opts.RetryWaitMin
	client.RetryWaitMax = opts.RetryWaitMax
	client.HTTPClient.Timeout = opts.Timeout
	client.HTTPClient.Transport = &oauth2.Transport{Source: tokens, Base: transport}
	client.Logger = nil
	// Hand back the last response when retries run out so that the failure
	// can be classified by its status code.
	client.ErrorHandler = retryablehttp.PassthroughErrorHandler
	client.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// A token request that Google rejected will not succeed on retry.
		var tokenErr *oauth2.RetrieveError
		if errors.As(err, &tokenErr) && tokenErr.Response != nil && tokenErr.Response.StatusCode < http.StatusInternalServerError {
			return false, nil
		}
		return checkRetry(ctx, resp, err)
	}

	var store reactionStore
	if opts.RedisAddress != "" {
		store = newRedisReactionStore(redis.NewClient(&redis.Options{
			Addr:     opts.RedisAddress,
			Password: opts.RedisPassword,
			DB:       opts.RedisDB,
		}), opts.RedisKeyPrefix, ttl)
		opts.Log.Info("reaction state shared via redis", "room", opts.Room)
	} else {
		store = newMemoryReactionStore(ttl)
	}

	return &reactionManager{
		lo:             opts.Log,
		metrics:        opts.Metrics,
		provider:       provider,
		room:           opts.Room,
		client:         client,
		apiBase:        endpoint.Scheme + "://" + endpoint.Host + "/v1",
		store:          store,
		firing:         ro.Firing,
		resolved:       ro.Resolved,
		removePrevious: ro.RemovePrevious,
	}, nil
}

// reactionTokenSource builds an OAuth2 token source for a Google Chat user.
// Only credential types that can act as a user are accepted, and each is
// loaded explicitly rather than through type auto-detection.
func reactionTokenSource(ctx context.Context, ro ReactionOpts) (oauth2.TokenSource, error) {
	var data []byte
	switch {
	case ro.CredentialsFile != "" && ro.CredentialsJSON != "":
		return nil, fmt.Errorf("set only one of reactions.credentials_file and reactions.credentials_json")
	case ro.CredentialsJSON != "":
		data = []byte(ro.CredentialsJSON)
	case ro.CredentialsFile != "":
		var err error
		if data, err = os.ReadFile(ro.CredentialsFile); err != nil {
			return nil, fmt.Errorf("reading reactions.credentials_file: %w", err)
		}
	default:
		return nil, fmt.Errorf("reactions require reactions.credentials_file or reactions.credentials_json: an incoming webhook cannot manage reactions")
	}

	var creds struct {
		Type         string `json:"type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
	}
	// The decode error is dropped on purpose: it can quote the credentials.
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("reactions credentials are not valid JSON")
	}

	switch creds.Type {
	case "authorized_user":
		if ro.ImpersonateUser != "" {
			return nil, fmt.Errorf("reactions.impersonate_user only applies to service_account credentials")
		}
		if creds.ClientID == "" || creds.ClientSecret == "" || creds.RefreshToken == "" {
			return nil, fmt.Errorf("authorized_user reactions credentials need client_id, client_secret and refresh_token")
		}
		cfg := oauth2.Config{
			ClientID:     creds.ClientID,
			ClientSecret: creds.ClientSecret,
			Endpoint:     google.Endpoint,
			Scopes:       []string{reactionsScope},
		}
		if creds.TokenURI != "" {
			cfg.Endpoint.TokenURL = creds.TokenURI
		}
		return cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: creds.RefreshToken}), nil
	case "service_account":
		if ro.ImpersonateUser == "" {
			return nil, fmt.Errorf("service_account reactions credentials need reactions.impersonate_user: Google Chat only accepts reactions from a user")
		}
		cfg, err := google.JWTConfigFromJSON(data, reactionsScope)
		if err != nil {
			return nil, fmt.Errorf("reactions service_account credentials are invalid")
		}
		cfg.Subject = ro.ImpersonateUser
		return cfg.TokenSource(ctx), nil
	default:
		return nil, fmt.Errorf("unsupported reactions credentials type %q: must be authorized_user or service_account", creds.Type)
	}
}

// validResourceName reports whether name follows the given collection layout
// (for example spaces/*/messages/*) and is safe to use as a request path.
// Names come from API responses and from shared state, never from config.
func validResourceName(name string, collections ...string) bool {
	parts := strings.Split(name, "/")
	if len(parts) != 2*len(collections) {
		return false
	}
	for i, part := range parts {
		if i%2 == 0 {
			if part != collections[i/2] {
				return false
			}
			continue
		}
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "?#%\\ \t\r\n") {
			return false
		}
	}
	return true
}

// apply updates the reactions for alerts that have already been delivered.
// It reports failures through logs and metrics only: a reaction must never
// turn a delivered notification into a dispatch error.
func (r *reactionManager) apply(ctx context.Context, events []reactionEvent) {
	for _, event := range events {
		switch event.status {
		case "firing":
			r.onFiring(ctx, event.key, event.message)
		case "resolved":
			r.onResolved(ctx, event.key)
		}
	}
}

// onFiring remembers message as the original notification of the alert and
// marks it as firing. Repeats and retries reuse the first message.
func (r *reactionManager) onFiring(ctx context.Context, key, message string) {
	if !validResourceName(message, "spaces", "messages") {
		r.skip("missing_message_name", "google chat did not return a message name, so its reaction cannot be managed")
		return
	}

	// Without a stored reference the reaction could never be updated again,
	// so a store failure skips it rather than leaving it stuck on firing.
	record, err := r.store.claim(ctx, key, message)
	if err != nil {
		r.fail("store", "recording original message for reactions failed", err)
		return
	}
	if r.firing == "" || record.FiringReaction != "" || record.Resolved {
		return
	}

	reaction, err := r.add(ctx, record.Message, r.firing)
	if errors.Is(err, errReactionExists) {
		return
	}
	if err != nil {
		r.fail("add", "adding firing reaction failed", err)
		return
	}
	r.count("add")
	if reaction == "" {
		return
	}

	record, err = r.store.setFiringReaction(ctx, key, reaction)
	if err != nil {
		r.fail("store", "recording firing reaction failed", err)
		return
	}
	// The alert resolved while the reaction was being added, so the delivery
	// that handled RESOLVED never saw it and could not remove it.
	if record.Resolved && r.removePrevious {
		r.removeFiring(ctx, reaction)
	}
}

// onResolved swaps the firing reaction on the original message for the
// resolved one. It is a no-op when the alert was already marked resolved.
func (r *reactionManager) onResolved(ctx context.Context, key string) {
	record, ok, err := r.store.get(ctx, key)
	if err != nil {
		r.fail("store", "looking up original message for reactions failed", err)
		return
	}
	if !ok {
		r.skip("missing_reference", "no original message is known for this resolved alert, leaving reactions untouched")
		return
	}
	if record.Resolved {
		return
	}

	done := true
	if r.removePrevious && record.FiringReaction != "" {
		done = r.removeFiring(ctx, record.FiringReaction)
	}
	if r.resolved != "" {
		_, err := r.add(ctx, record.Message, r.resolved)
		switch {
		case err == nil:
			r.count("add")
		case errors.Is(err, errReactionExists):
		case errors.Is(err, errReactionNotFound):
			r.lo.Info("original google chat message no longer exists, skipping resolved reaction", "room", r.room)
		default:
			r.fail("add", "adding resolved reaction failed", err)
			done = false
		}
	}
	// After a failure the record stays unresolved, so that an Alertmanager
	// retry of this notification can finish the update.
	if !done {
		return
	}

	updated, err := r.store.markResolved(ctx, key)
	if err != nil {
		r.fail("store", "recording resolved state for reactions failed", err)
		return
	}
	// A FIRING delivery added its reaction after the record was read above.
	if r.removePrevious && updated.FiringReaction != "" && updated.FiringReaction != record.FiringReaction {
		r.removeFiring(ctx, updated.FiringReaction)
	}
}

// removeFiring deletes a firing reaction and reports whether it is gone.
func (r *reactionManager) removeFiring(ctx context.Context, reaction string) bool {
	err := r.remove(ctx, reaction)
	if errors.Is(err, errReactionNotFound) {
		return true
	}
	if err != nil {
		r.fail("remove", "removing firing reaction failed", err)
		return false
	}
	r.count("remove")
	return true
}

// add creates a reaction on message and returns its resource name, which is
// empty if Google Chat created the reaction without reporting a usable name.
func (r *reactionManager) add(ctx context.Context, message, emoji string) (string, error) {
	if !validResourceName(message, "spaces", "messages") {
		return "", &reactionError{reason: "store", detail: "stored google chat message name is invalid"}
	}
	body, err := json.Marshal(chatv1.Reaction{Emoji: &chatv1.Emoji{Unicode: emoji}})
	if err != nil {
		return "", &reactionError{reason: "request", detail: "encoding google chat reaction failed"}
	}
	data, err := r.call(ctx, http.MethodPost, message+"/reactions", body)
	if err != nil {
		return "", err
	}

	var created struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &created); err != nil || !validResourceName(created.Name, "spaces", "messages", "reactions") {
		return "", nil
	}
	return created.Name, nil
}

// remove deletes the reaction with the given resource name.
func (r *reactionManager) remove(ctx context.Context, reaction string) error {
	if !validResourceName(reaction, "spaces", "messages", "reactions") {
		return &reactionError{reason: "store", detail: "stored google chat reaction name is invalid"}
	}
	_, err := r.call(ctx, http.MethodDelete, reaction, nil)
	return err
}

// call sends one authenticated Chat API request. Errors never carry the
// request, the response body or anything derived from the credentials.
func (r *reactionManager) call(ctx context.Context, method, name string, body []byte) ([]byte, error) {
	var rawBody any
	if body != nil {
		rawBody = body
	}
	req, err := retryablehttp.NewRequestWithContext(ctx, method, r.apiBase+"/"+name, rawBody)
	if err != nil {
		return nil, &reactionError{reason: "request", detail: "building google chat reaction request failed"}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// When retries run out the client hands back the last response, so a
	// failure is classified by its status whenever there is a response.
	resp, err := r.client.Do(req)
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if resp == nil {
		var tokenErr *oauth2.RetrieveError
		if errors.As(err, &tokenErr) {
			detail := "fetching google chat access token failed"
			if tokenErr.ErrorCode != "" {
				detail += ": " + tokenErr.ErrorCode
			}
			return nil, &reactionError{reason: "auth", detail: detail}
		}
		return nil, &reactionError{reason: "request", detail: "sending google chat reaction request failed"}
	}

	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxChatResponseSize))
		if err != nil {
			// The request succeeded; only the response could not be read.
			return nil, nil
		}
		return data, nil
	case code == http.StatusConflict:
		return nil, errReactionExists
	case code == http.StatusNotFound:
		return nil, errReactionNotFound
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return nil, &reactionError{reason: "auth", detail: fmt.Sprintf("google chat denied the reaction request with status %d", code)}
	case code == http.StatusTooManyRequests:
		return nil, &reactionError{reason: "rate_limited", detail: "google chat rate limited the reaction request"}
	default:
		return nil, &reactionError{reason: "api", detail: fmt.Sprintf("google chat returned status %d for the reaction request", code)}
	}
}

func (r *reactionManager) count(action string) {
	r.metrics.Increment(fmt.Sprintf(`reactions_total{provider="%s", room="%s", action="%s"}`, r.provider, r.room, action))
}

func (r *reactionManager) skip(reason, msg string) {
	r.lo.Info(msg, "room", r.room)
	r.metrics.Increment(fmt.Sprintf(`reactions_skipped_total{provider="%s", room="%s", reason="%s"}`, r.provider, r.room, reason))
}

// fail records a reaction failure. The reason label comes from the API error
// when there is one and falls back to the failed action otherwise.
func (r *reactionManager) fail(action, msg string, err error) {
	reason := action
	var apiErr *reactionError
	switch {
	case errors.As(err, &apiErr):
		reason = apiErr.reason
	case errors.Is(err, errReactionNotFound):
		reason = "not_found"
	}
	r.lo.Warn(msg, "error", err, "room", r.room)
	r.metrics.Increment(fmt.Sprintf(`reactions_errors_total{provider="%s", room="%s", action="%s", reason="%s"}`, r.provider, r.room, action, reason))
}
