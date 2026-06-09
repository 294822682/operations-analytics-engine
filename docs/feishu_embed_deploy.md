# Feishu BI Embed Deployment

## Entry URL

After the FastAPI service is deployed to a public HTTPS host, embed this URL in Feishu:

```text
https://<host>/dashboard/daily/latest/feishu-link
```

For range queries, the page redirects to:

```text
https://<host>/dashboard/daily/trends/prototype?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

## Render Blueprint

This repository includes `render.yaml` for a single Render web service:

```text
uvicorn oae.api.dashboard_app:app --host 0.0.0.0 --port $PORT
```

The service health check path is:

```text
/healthz
```

The Blueprint uses the `starter` plan because the service needs a persistent disk for runtime BI data. Render persistent disks are not available to build commands, so do not rely on the disk during dependency installation.

## Data Directory

The BI service reads dashboard data from:

```text
output/sql_reports/feishu_dashboard_source_latest_*.tsv
```

`output/` is treated as runtime data, not source code. The Render Blueprint mounts a persistent disk at:

```text
/opt/render/project/src/output
```

Before Feishu acceptance, sync the approved dashboard source TSV files into:

```text
/opt/render/project/src/output/sql_reports/
```

If the service starts but `/dashboard/daily/latest/feishu-link` has no data, verify that the latest approved TSV exists in that directory before changing UI code.

To prepare a local upload bundle that only includes approved dashboard source TSV files:

```bash
PYTHONPATH=src python scripts/prepare_feishu_embed_data.py --repo-root "$PWD" --output-dir /tmp/oae-feishu-embed-data
```

Upload the generated `output/sql_reports/feishu_dashboard_source_latest_*.tsv` files from that bundle to the Render persistent disk path above. Do not upload the whole local `output/` directory.

## Acceptance Check

After deployment and data sync, run:

```bash
PYTHONPATH=src python scripts/verify_feishu_embed.py https://<host>
```

The verifier checks:

- `/healthz`
- `/dashboard/daily/latest`
- `/dashboard/daily/latest/feishu-link`
- `/dashboard/daily/trends`

The final Feishu embed URL is valid only after this verifier returns `"status": "ok"`.

## Local Verification

```bash
PYTHONPATH=src OAE_REPO_ROOT="$PWD" .venv/bin/python -m uvicorn oae.api.dashboard_app:app --host 127.0.0.1 --port 8030
```

Open:

```text
http://127.0.0.1:8030/dashboard/daily/latest/feishu-link
```
