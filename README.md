# Deploying rdbear_notouch_explorer.py to Railway

## Files in this bundle
- `requirements.txt` — Python deps (websockets, numpy, requests)
- `Procfile` — declares a `worker` process (no web/HTTP port needed)
- `railway.toml` — Railway build/deploy config, no healthcheck (this isn't a web service)
- `runtime.txt` — pins Python 3.11
- `.env.example` — the env vars to set in Railway's Variables tab
- `.gitignore` — keeps secrets and generated data out of git

## Setup steps
1. Put `rdbear_notouch_explorer.py` in the same folder as these files, and push
   the whole folder to a GitHub repo (or use `railway up` from the CLI directly).
2. On [railway.app](https://railway.app), create a **New Project → Deploy from GitHub repo**
   (or `railway init` + `railway up` from this folder via the CLI).
3. Since there's no `web` process, Railway will auto-detect the Procfile's
   `worker` entry via `railway.toml`'s `startCommand`. In the service settings,
   confirm the service type shows as a background worker, not a web service
   (no public domain / port needed).
4. Under **Variables**, add the entries from `.env.example` with your real values:
   - `DERIV_APP_ID`, `DERIV_API_TOKEN` — required (must be from a *new*
     developers.deriv.com app; legacy app_ids like `1089` won't work with this API).
   - `DERIV_ACCOUNT_TYPE` — `demo` or `real`.
   - Leave `COLLECT_MINUTES=0` to run indefinitely, or set a number of minutes.
5. Deploy. Logs (tick collection + periodic analysis reports) show up in the
   Railway **Deployments → Logs** tab.

## Important: Railway's filesystem is ephemeral
This script writes `*_ticks.csv` and `*_notouch_analysis.json` to local disk.
On Railway, local disk does **not** persist across redeploys/restarts (and isn't
shared if you scale to multiple instances). If you need the CSV/JSON to survive
restarts or be accessible after the fact, you have two options:

- **Railway Volumes**: attach a persistent volume to the service and point the
  script at a path under it (you'd need to tweak `TICK_CSV_PATH`/`REPORT_JSON_PATH`
  in the script, e.g. `/data/rdbear_ticks.csv`, and mount a volume at `/data`).
- **External storage**: modify the script to push the CSV/JSON to S3, a database,
  or similar on each save, if you need durability without volumes.

Since this script is a long-running collector (not a request/response service),
a single always-on worker instance (no autoscaling) is the right fit — this is
already reflected in `railway.toml` (no healthcheck, `ON_FAILURE` restarts only).
