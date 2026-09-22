# GCloud Cloud Shell - Render Active Mode

Runs `gcloud cloud-shell ssh` on Render with auto-reconnect. Never sleeps.
Quota hit → auto-switch account from MongoDB.

## How it works

1. `gcloud cloud-shell ssh --ssh-flag="-o ServerAliveInterval=30" --ssh-flag="-o ServerAliveCountMax=60"` keeps connection alive
2. When gcloud restarts itself or SSH drops → auto-reconnect
3. Short session (< 60s) = quota hit → after 5 failures, switch to next account
4. Gcloud config saved/restored from MongoDB across restarts
5. Live log at `/log` (xterm.js terminal)

## Deploy

1. Set `MONGO_URI` in Render env vars
2. Push to GitHub → Render auto-deploys Docker
3. Add accounts:
```bash
curl -X POST https://your-app.onrender.com/accounts \
  -H "Content-Type: application/json" \
  -d '{"email":"user1@gmail.com","default":true}'

curl -X POST https://your-app.onrender.com/accounts \
  -H "Content-Type: application/json" \
  -d '{"email":"user2@gmail.com"}'
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Status + uptime |
| GET | `/health` | Health check |
| GET | `/log` | **Live terminal UI** |
| GET | `/log/stream` | SSE log stream |
| GET | `/session` | Current session info |
| GET | `/accounts` | List accounts |
| POST | `/accounts` | Add `{"email":"..."}` |
| DELETE | `/accounts/<email>` | Remove |
| POST | `/exec` | Run `{"command":"..."}` |
| POST | `/config/save` | Save gcloud config to MongoDB |
| POST | `/config/restore` | Restore from MongoDB |

## Env Vars

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | required | MongoDB URI |
| `MONGO_DB` | `gcloud_shell` | Database name |
| `FAIL_THRESHOLD` | `5` | Failures before account switch |
| `SHORT_SESSION_SECONDS` | `60` | Sessions shorter = failure |
| `SSH_ALIVE_INTERVAL` | `30` | SSH keepalive interval |
| `SSH_ALIVE_COUNT` | `60` | SSH keepalive count |
| `RECONNECT_DELAY` | `10` | Seconds between reconnects |
| `PORT` | `8080` | HTTP port |
