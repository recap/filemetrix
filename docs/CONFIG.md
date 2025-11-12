# FileMetrix Configuration Reference

This document lists environment variables and Dynaconf settings used by FileMetrix with example values. Use `conf/settings.example.toml` or `conf/settings.production.toml` as templates.

## Application / runtime
- API_PREFIX: string
  - Example: `/api/v1`
  - Purpose: Router prefix for the API endpoints.

- EXPOSE_PORT: int
  - Example: `1966`
  - Purpose: HTTP port the app binds to.

- BUILD_DATE: string
  - Example: `2025-12-10`
  - Purpose: Build identifier shown in OpenAPI and startup emails.

- FILEMETRIX_SERVICE_API_KEY
  - Example: `changeme` (replace with a secure key)
  - Purpose: API key used for protected routes.

## Database
- DB_USER
  - Example: `fms`
- DB_PASSWORD
  - Example: `super-secret` (do NOT store in git)
- DB_HOST
  - Example: `postgres` or `db.internal.company.org`
- DB_PORT
  - Example: `5432`
- DB_NAME
  - Example: `filemetrix`

The DB connection string is built as: `postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}`.

## Email / SMTP (optional)
- MAIL_HOST
  - Example: `maildev` or `smtp.gmail.com`
- MAIL_PORT
  - Example: `1025` (maildev) or `587` (TLS)
- MAIL_USR / MAIL_PASS
  - Use app-specific passwords or a secrets manager.
- MAIL_FROM
  - Example: `e.indarto@gmail.com`
- MAIL_TO
  - Example: `["eko.indarto@dans.knaw.nl"]`
- MAIL_USE_TLS / MAIL_USE_SSL / MAIL_USE_AUTH
  - Example: `true / false / true`

## Other
- PID_FETCHER_URL
  - Example: `https://pid-fetcher.example.org/`
  - Purpose: External service used to retrieve file-level metadata for a PID.

- PKL_TOKEN_FILE
  - Example: `/var/lib/filemetrix/token.pkl`
  - Purpose: Path to store OAI-PMH resumption token for interrupted harvests.

- LOG_LEVEL / LOG_FILE
  - Example: `20` (INFO) and `/var/log/filemetrix/fms.log`

- OTLP_ENABLE
  - Example: `false` - enable OpenTelemetry export if true.

## Notes
- Preferred method: mount `conf/settings.toml` into the container at `/home/akmi/fms/conf/settings.toml` or set environment variables in your orchestration system (Kubernetes Secrets, Docker Compose env_file, etc.).
- Avoid committing secrets to the repository. Use secret managers or environment injection in CI/CD.

