# KQSX API

FastAPI service that surfaces Vietnamese lottery (Kết quả xổ số) summaries from Supabase, ready to plug into OpenAI actions or other conversational agents.

## Features
- `/v1/kqsx/summary` returns a structured object + LLM-friendly text for daily results (per region or nationwide fallback).
- Automatically triggers the bundled scraper to fetch and publish fresh results to Supabase when the requested date is missing.
- `/privacy_policy` exposes the privacy notice required for OpenAI action submissions.
- Reads Supabase data via REST API using environment variables defined in `.env`.
- Docker image based on Python 3.11, serving with uvicorn on port 8090.

## Requirements
- Python 3.11+ (for local development)
- Supabase project with lottery tables populated.
- Docker (optional, for containerized deployment)
- Environment variables (see `.env.example` or section below).

## Local Setup
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
uvicorn api:app --reload --port 8090
```
Visit `http://localhost:8090/healthz` for a quick status check.

## Environment Variables
Create a `.env` (or `.env.production` for deployment):
```
VITE_SUPABASE_URL=https://your-project.supabase.co
VITE_SUPABASE_SERVICE_ROLE_KEY=service-role-or-publishable-key
# Optional: VITE_SUPABASE_PUBLISHABLE_KEY=...
OLLAMA_HOST=http://127.0.0.1:11434  # Only if scraping needs it
```
Never commit production keys—use your hosting platform's secret manager or `--env-file` flag.

## Docker
Build and run the containerized app:
```bash
docker build -t kqsx-api .
docker run --rm -p 8090:8090 --env-file .env kqsx-api
```

## API Endpoints
- `GET /healthz` – Service status.
- `GET /privacy_policy` – Privacy policy payload.
- `GET /v1/kqsx/summary?date=YYYY-MM-DD&region=mn|mt|mb` – Lottery summary for the given day. Omit `region` to merge all regions; the endpoint will fallback to recent days if requested date has no data. The API will attempt an on-demand scrape (and Supabase sync) for the requested date before falling back.

## Deployment Snapshot (Ubuntu)
1. Install Docker & Nginx, open firewall.
2. Copy `.env.production` to the server (restrict permissions).
3. `git clone`, `docker build`, and run container (or create a systemd service).
4. Configure Nginx reverse proxy to `http://127.0.0.1:8090` and secure with Certbot.
5. Verify via `curl -I https://your.domain/healthz`.

## Contributing
Pull requests are welcome. Please ensure:
- Added dependencies go into `requirements.txt`.
- New endpoints include brief documentation here.
- Tests or manual steps are noted in PR descriptions.

## License
MIT (adjust if project requires a different license).*** End Patch
