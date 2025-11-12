#!/usr/bin/env python3
"""Pre-start environment validator for FileMetrix.

Behavior:
- By default the script verifies required env vars (DB_*) are present and exits 1 if missing.
- If ENV or APP_ENV is set to a development value (dev/development/local) or
  the environment variable SKIP_ENV_VALIDATION is set, DB connectivity checks are skipped
  to support local development flows.
- CLI flags:
  --skip-db   : skip attempting to connect to the DB
  --strict    : in addition to presence checks, attempt a DB connection and fail if it cannot connect

This script is intended to be run inside the container as part of the Docker CMD in production.
"""
import os
import sys
import argparse
import logging
import time

REQUIRED = [
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASSWORD",
]

DEV_ENV_VALUES = {"dev", "development", "local"}


def is_dev_mode() -> bool:
    env = os.environ.get("ENV") or os.environ.get("APP_ENV") or os.environ.get("FLASK_ENV") or os.environ.get("FASTAPI_ENV")
    if env and env.lower() in DEV_ENV_VALUES:
        return True
    if os.environ.get("SKIP_ENV_VALIDATION"):
        return True
    return False


def check_required_vars() -> list:
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    return missing


def _attempt_db_connection() -> bool:
    # single attempt to connect, returns True/False
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT", 5432)),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            dbname=os.environ.get("DB_NAME") or "filemetrix",
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception as e:
        msg = str(e)
        logging.debug("DB connectivity attempt failed: %s", msg)
        return False


def check_db_connection_with_retry(timeout: int = 30, interval: int = 2) -> bool:
    """Retry DB connection until success or timeout (seconds).

    Returns True if a connection was successful within timeout, otherwise False.
    """
    deadline = time.time() + timeout
    attempt = 1
    while time.time() <= deadline:
        print(f"DB connectivity attempt #{attempt}...")
        ok = _attempt_db_connection()
        if ok:
            print("DB connectivity OK")
            return True
        wait = min(interval, max(1, int(deadline - time.time())))
        if wait <= 0:
            break
        print(f"DB not ready yet; retrying in {wait} seconds...")
        time.sleep(wait)
        attempt += 1
    print(f"DB connectivity not available after {timeout} seconds.")
    return False


def check_db_connection() -> bool:
    # Backwards-compatible single-attempt wrapper
    return _attempt_db_connection()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate required environment variables for FileMetrix")
    parser.add_argument("--skip-db", action="store_true", help="Skip attempting a DB connection")
    parser.add_argument("--strict", action="store_true", help="Run a DB connectivity check and fail on error")
    parser.add_argument("--db-wait-timeout", type=int, default=None, help="Override DB wait timeout in seconds")
    parser.add_argument("--db-wait-interval", type=int, default=None, help="Override DB wait interval in seconds")
    args = parser.parse_args(argv)

    dev = is_dev_mode()
    if dev:
        print("Detected development mode or SKIP_ENV_VALIDATION set — running lighter validation (DB checks skipped).")

    # Check required env vars
    missing = check_required_vars()
    if missing:
        print("Missing required environment variables:", ", ".join(missing), file=sys.stderr)
        if dev:
            print("Development mode: continuing despite missing vars.")
        else:
            print("Set the required environment variables or provide a conf/settings.toml.")
            return 1

    # Decide whether to run DB checks
    should_check_db = not args.skip_db and (args.strict or not dev)

    if should_check_db:
        # Determine timeout/interval from CLI or env
        timeout = args.db_wait_timeout if args.db_wait_timeout is not None else int(os.environ.get("DB_WAIT_TIMEOUT", 30))
        interval = args.db_wait_interval if args.db_wait_interval is not None else int(os.environ.get("DB_WAIT_INTERVAL", 2))

        # In strict mode, perform immediate single attempt first then retry (honor strict by failing fast?)
        # We'll perform retries up to timeout to be resilient when DB and app start together.
        print(f"Checking DB connectivity with timeout={timeout}s and interval={interval}s...")
        ok = check_db_connection_with_retry(timeout=timeout, interval=interval)
        if not ok:
            # Provide helpful message; in dev mode continue
            print("DB connectivity check failed.", file=sys.stderr)
            if dev:
                print("Development mode: continuing despite DB connectivity failure.")
            else:
                return 2

    print("Environment validation passed.")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
