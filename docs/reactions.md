# Reactions on resolved alerts

With reactions enabled, calert marks the state of an alert on its **original**
Google Chat message, so a room shows at a glance which alerts are still open:

| Alertmanager event | What calert does |
|--------------------|------------------|
| `firing` | Sends the notification as usual, remembers the message and adds the `firing` reaction (default 🔥) to it. |
| `resolved` | Sends the resolved notification as usual (same thread when `threaded_replies = true`), removes the `firing` reaction from the original message and adds the `resolved` reaction (default ✅). |

The feature is **off by default**. Nothing changes for an existing
installation until `reactions.enabled = true` is set for a room.

## Why a webhook is not enough

An incoming webhook URL can only create messages. Reactions go through the
Google Chat API methods
[`spaces.messages.reactions.create`](https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces.messages.reactions/create)
and
[`spaces.messages.reactions.delete`](https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces.messages.reactions/delete),
and Google only accepts those calls **authenticated as a user**. A Chat app or
a plain service account cannot add reactions.

calert therefore keeps posting notifications through the webhook and uses a
second, authenticated client only for reactions. Consequences to be aware of:

- The reaction is shown as coming from the user calert authenticates as. Use a
  dedicated account (for example `alerts-bot@example.com`).
- That user must be a member of every space that has reactions enabled.
- Only unicode emoji are supported, not custom emoji.
- Reactions need `threading_mode = "alert"` (the default). In `group` mode one
  message aggregates many alerts, so reactions are ignored with a warning.

## Google Cloud setup

1. Create or pick a Google Cloud project and **enable the Google Chat API**
   (APIs & Services → Library → Google Chat API).
2. On the Chat API page, complete the **Configuration** tab (app name, avatar,
   description). Google requires this before the API accepts calls.
3. Configure the **OAuth consent screen**. Choose the *Internal* user type if
   you can: refresh tokens of an app left in *Testing* expire after 7 days.
4. Add the bot user to each space that should get reactions.
5. Create credentials using **one** of the two options below.

The only OAuth scope calert requests is:

```
https://www.googleapis.com/auth/chat.messages.reactions
```

### Option A: OAuth user credentials (`authorized_user`)

Works on any Workspace edition and needs no admin.

1. Create an OAuth client ID of type *Desktop app* and download its JSON.
2. Signed in as the bot user, run a one-time consent flow that requests the
   scope above and yields a refresh token. With the gcloud CLI:

   ```sh
   gcloud auth application-default login \
     --client-id-file=client_secret.json \
     --scopes=https://www.googleapis.com/auth/chat.messages.reactions
   ```

   If gcloud asks for additional scopes, add them to the list. Any other OAuth
   flow that returns a refresh token for your client works as well.
3. Use the resulting file (`application_default_credentials.json`) as the
   credentials. It looks like this:

   ```json
   {
     "type": "authorized_user",
     "client_id": "…apps.googleusercontent.com",
     "client_secret": "…",
     "refresh_token": "…"
   }
   ```

### Option B: service account with domain-wide delegation

For Workspace domains whose admin prefers not to hand out a refresh token.

1. Create a service account and a JSON key for it.
2. A Workspace super admin authorises the service account's client ID for the
   scope above (Admin console → Security → API controls → Domain-wide
   delegation).
3. Set `impersonate_user` to the bot user. calert then signs a token request
   on behalf of that user; the service account never reacts as itself.

## Configuration

```toml
[providers.prod_alerts]
type = "google_chat"
endpoint = "https://chat.googleapis.com/v1/spaces/xxx/messages?key=...&token=..."
threaded_replies = true

[providers.prod_alerts.reactions]
enabled = true
firing = "🔥"                # "" adds no reaction on firing
resolved = "✅"              # "" adds no reaction on resolved
remove_previous = true       # remove the firing reaction when resolved
credentials_file = "/etc/calert/chat-credentials.json"
# impersonate_user = "alerts-bot@example.com" # service accounts only
ttl = "168h"                 # how long an original message is remembered

# Optional. Required for reactions when running more than one calert replica.
[providers.prod_alerts.redis]
address = "redis:6379"
```

| Key | Explanation | Default |
|-----|-------------|---------|
| `reactions.enabled` | Turns the feature on for the room. | `false` |
| `reactions.firing` | Unicode emoji added to the original message while the alert fires. | `🔥` |
| `reactions.resolved` | Unicode emoji added to the original message once the alert resolves. | `✅` |
| `reactions.remove_previous` | Remove the firing reaction when the alert resolves. | `true` |
| `reactions.credentials_file` | Path to `authorized_user` or `service_account` credentials. | - |
| `reactions.credentials_json` | The same credentials inline. Meant for environment variables. | - |
| `reactions.impersonate_user` | User a service account acts as. Required for, and only valid with, `service_account` credentials. | - |
| `reactions.ttl` | How long the original message of an alert is remembered. | `168h` |

Exactly one of `credentials_file` and `credentials_json` must be set. Like
every other option, they can come from the environment, which is the easiest
way to keep the secret out of the config file:

```sh
CALERT_PROVIDERS__PROD_ALERTS__REACTIONS__ENABLED=true
CALERT_PROVIDERS__PROD_ALERTS__REACTIONS__CREDENTIALS_JSON='{"type":"authorized_user",...}'
```

calert validates the credentials at startup and refuses to start when
reactions are enabled without usable ones. Access tokens are fetched lazily,
cached and refreshed automatically, through `proxy_url` when one is set.

Alertmanager must send resolved notifications (`send_resolved: true`).

## How the original message is found

calert identifies an alert lifecycle by its `fingerprint` and `startsAt`, the
same pair that already drives per-alert threading. The first `firing`
notification of a lifecycle is the original message; its Google Chat resource
name is stored under a room-scoped SHA-256 digest of that pair.

- **In memory (default):** state lives in the calert process. It is lost on
  restart and is not shared, so use it with a single replica only.
- **Redis:** set `providers.<room>.redis.*` and all replicas share the state.
  Any replica can then resolve an alert that another one announced.

Every `firing` notification extends the retention of its record, so `ttl` only
has to be longer than Alertmanager's `repeat_interval`.

## Behaviour in edge cases

Reactions are updated after every notification of a webhook payload has been
sent, and their failures are only logged and counted. **A reaction can never
delay, fail or suppress an alert notification.**

| Situation | Result |
|-----------|--------|
| Duplicate or repeated `firing` | The notification is sent as before. The reaction stays on the first message only. |
| Duplicate `resolved` | The notification is sent as before. Reactions are not touched again. |
| `resolved` without a known original message (restart with in-memory state, expired `ttl`, feature enabled mid-alert) | Notification is sent, reactions are skipped, `reactions_skipped_total{reason="missing_reference"}` increments. |
| Original message was deleted | Notification is sent, nothing else happens. |
| Chat API error, rate limit or timeout | Retried with the room's `retry_*` settings, then logged and counted. A later retry of the same notification finishes the update. |
| Rejected credentials | Not retried. Logged and counted with `reason="auth"`. |
| Redis unavailable | Notification is sent. No reaction is added, because it could not be removed again later. |
| Several replicas handle the same alert at once | They agree on one original message and converge on the same reactions. |

Alertmanager sends a resolved notification once. If the Chat API is still
failing after the retries, the firing reaction stays on that message.

## Metrics

- `calert_reactions_total{action="add|remove"}` counts reactions added and
  removed.
- `calert_reactions_errors_total{action, reason}` counts failed updates.
  `reason` is one of `auth`, `rate_limited`, `api`, `request`, `not_found` or
  `store`.
- `calert_reactions_skipped_total{reason}` counts alerts whose reaction could
  not be managed: `missing_reference` or `missing_message_name`.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `fetching google chat access token failed: invalid_grant` | The refresh token was revoked or expired (consent screen in *Testing*), or the service account is not authorised for the scope / user. |
| `google chat denied the reaction request with status 403` | The Chat API is not enabled or configured in the project, or the user is not a member of the space. |
| `reason="missing_message_name"` | `endpoint` does not point at the Google Chat API, so the response carries no message name. |
| Firing reaction never removed after a restart | In-memory state was lost. Configure Redis. |

## Migrating an existing installation

No action is needed to keep the current behaviour: the new options default to
disabled and webhook-only rooms never use the authenticated API.

To adopt reactions:

1. Upgrade calert. Existing configuration keeps working unchanged.
2. Complete the Google Cloud setup above and add the bot user to the space.
3. Provide the credentials as a file or through
   `CALERT_PROVIDERS__<ROOM>__REACTIONS__CREDENTIALS_JSON`.
4. Add the `[providers.<room>.reactions]` section with `enabled = true`.
5. Running several replicas? Configure `[providers.<room>.redis]` first.
6. Restart calert. For an alert that was already firing, the next repeated
   `firing` notification becomes the tracked message and gets the reaction.
   An alert that resolves before that is skipped once.
