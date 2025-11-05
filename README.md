# FileMetrix Service

Behind every file is a Metrix. Harvest, identify, and register file-level metadata at scale.

Live demo: https://filemetrix.labs.dansdemo.nl/docs

## Functionalities
- Harvest dataset identifiers from OAI-PMH repositories (via `sickle`).
- Store datasets and file metadata in PostgreSQL (SQLModel / SQLAlchemy).
- Provide REST API endpoints (FastAPI) for repository, dataset and file-metadata queries.
- Re3data lookup and PID-to-repository resolution using `datahugger`.
- Email notifications for startup, harvest start/completion/errors using Gmail SMTP.
- Metrics and aggregation endpoints (counts grouped by MIME type, size per repo, publication-month grouping).

## Dockerize
- Build the image:
```bash
docker build -t filemetrix-service .
```

- Run the container:
```bash
docker run -e BUILD_DATE="$(date -I)" -p 1966:1966 --name filemetrix \
  -v "$(pwd)/logs:/home/akmi/fms/logs" filemetrix-service
```

- Or use `docker-compose`:
```bash
docker-compose up -d --build
```
Ensure your `docker-compose.yaml` services and volumes are configured as needed.

- Docker Hub images: https://hub.docker.com/repository/docker/ekoindarto/filemetrix/general

Notes:
- The provided `Dockerfile` creates a venv and uses the `uv` package manager to install Python deps and copy `src` into the image.
- Mount a `conf` directory (or set `BASE_DIR`) so `dynaconf` can read runtime settings from `conf/*`.

## How to run (local / development)

Prerequisites:
- Python >= 3.12.8
- PostgreSQL accessible and credentials set via the configuration in `conf/*` or environment variables.

1. Clone repo and set `BASE_DIR` if needed:
```bash
git clone <repo-url>
cd filemetrix
export BASE_DIR="$(pwd)"
```

2. Install and prepare environment (using `uv` package manager described below) or use standard venv/pip:

- With `uv`:
```bash
uv venv .venv
uv sync --frozen --no-cache
```

- Without `uv` (standard):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # if present or install packages from pyproject.toml
# or
pip install -e .
```

3. Run the app:
- Direct:
```bash
python -m src.filemetrix.main
```
- Or with `uvicorn`:
```bash
uvicorn src.filemetrix.main:app --host 0.0.0.0 --port 1966 --reload
```

4. API will be available at: `http://localhost:1966/`. Check logs in `logs/fms.log`.

## UV (astral-sh/uv) — Python package manager used by this project
- Project uses `uv` to produce reproducible, minimal Python environments (see `pyproject.toml` and `uv.lock`).
- Common `uv` commands used in this repo:
  - `uv venv .venv` — create a virtual environment at `./.venv`.
  - `uv sync --frozen --no-cache` — install packages into the venv from `uv.lock`/`pyproject.toml`.

More: https://github.com/astral-sh/uv

## Installing the `uv` binary

Manual (download release and install):
1. Get the latest release from the `uv` releases page (GitHub). Pick the binary for your platform.
```bash
# example for macOS amd64; replace TAG and asset name as needed
curl -LO "https://github.com/astral-sh/uv/releases/download/<TAG>/uv-macos-amd64"
```

2. Make it executable and move it into your PATH:
```bash
chmod +x uv-macos-amd64
sudo mv uv-macos-amd64 /usr/local/bin/uv
```

3. Verify installation:
```bash
uv --version
```

Automatic install (Linux/macOS) — example using latest from GitHub (adjust as needed):
```bash
# This is a convenience example; inspect before running.
curl -sL "https://raw.githubusercontent.com/astral-sh/uv/main/scripts/install.sh" | bash
```

## Configuration
- Settings live in `conf/settings.toml` (see `conf/`), or can be provided via environment variables. Ensure DB credentials and mail settings are configured before running the service.

## Notes and troubleshooting
- If using Gmail SMTP, ensure the configured account allows SMTP access (app password or appropriate settings).
- If PostgreSQL is remote, confirm network and firewall settings.
- Logs are rotated daily (see `logs/fms.log`).

<!-- end of README -->
