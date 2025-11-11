import logging
import os
import sys
from datetime import datetime

import asyncio

from src.filemetrix.api.v1 import repo_workflow_controller, repo_discovery, repo_metrics, pid_fetcher
from src.filemetrix.infra.commons import app_settings, send_mail
from src.filemetrix.infra.db import ensure_database_exists, create_tables

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["BASE_DIR"] = os.getenv("BASE_DIR", base_dir)

from akmi_utils import commons as a_commons


from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.cors import CORSMiddleware

APP_NAME = os.environ.get("APP_NAME", "Filemetrix Service")
EXPOSE_PORT = os.environ.get("EXPOSE_PORT", 1966)
OTLP_GRPC_ENDPOINT = os.environ.get("OTLP_GRPC_ENDPOINT", "http://localhost:4317")

RELOAD_ENABLE = os.environ.get("RELOAD_ENABLE", "false").lower() == "true"


api_keys = [app_settings.FILEMETRIX_SERVICE_API_KEY]
security = HTTPBearer()



def auth_header(
    request: Request,
    auth_cred: Annotated[HTTPAuthorizationCredentials, Depends(security)],
):
    if not auth_cred or auth_cred.credentials not in api_keys:
            return HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden"
            )


project_details = a_commons.get_project_details(
    base_dir=os.getenv("BASE_DIR"),
    keys=["name", "version", "description", "title"],
)

import logging
from logging.handlers import TimedRotatingFileHandler
import os

log_dir = os.path.join(os.environ.get("BASE_DIR", "."), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "fms.log")

handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",  # rotate every second for testing
    interval=1,
    backupCount=7,
    encoding="utf-8",
    utc=True
)
handler.suffix = "%Y-%m-%d"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[handler]
)

@asynccontextmanager
async def lifespan(application: FastAPI):
    logging.info("start up")
    subject_success = "FileMetrix Service Startup Success"
    body_success = f"FileMetrix Service started successfully on {datetime.now().isoformat()}. Version: {project_details['version']}, Build Date: {build_date}."
    subject_error = "FileMetrix Service Startup Error"
    try:
        ensure_database_exists()
        create_tables()
        send_mail(subject_success, body_success)
        yield
    except Exception as e:
        error_body = f"FileMetrix Service failed to start on {datetime.now().isoformat()} with error: {str(e)}. Version: {project_details['version']}, Build Date: {build_date}."
        send_mail(subject_error, error_body)
        logging.error(f"Startup error: {e}")
        raise


build_date = os.environ.get("BUILD_DATE", "unknown")

tags_metadata = [
    {
        "name": "PID Fetcher",
        "description": "Fetching Persistent Identifiers (PIDs) for resources",
    },
    {
        "name": "Repo Discovery",
        "description": "Discovering repositories and their metadata",
    },
    {
        "name": "Repo Metrics",
        "description": "Querying metrics and statistics about harvested data",
    },
    {
        "name": "Repo Management",
        "description": "Managing repository workflows and operations",
    },

]
app = FastAPI(
    title=project_details['title'],
    description=project_details['description'],
    version=f"{project_details['version']} (Build Date: {build_date})",
    lifespan=lifespan,
    openapi_tags=tags_metadata,
)

@app.exception_handler(StarletteHTTPException)
async def custom_404_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(status_code=404, content={"message": "Endpoint not found"})
    return JSONResponse(status_code=exc.status_code, content={"message": exc.detail})


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(pid_fetcher.router, tags=["PID Fetcher"], prefix="")
app.include_router(repo_discovery.router, tags=["Repo Discovery"], prefix="")

app.include_router(repo_metrics.router, tags=["Repo Metrics"], prefix="")

app.include_router(repo_workflow_controller.router, tags=["Repo Management"], prefix="", dependencies=[Depends(auth_header)])


@app.get("/", include_in_schema=False)
async def root():
    logging.info("root route")
    return JSONResponse(
        status_code=200,
        content={
            "message": "Welcome to the FileMetrix Service",
            "version": project_details['version'],
            "build_date": build_date,
            "app_name": project_details['title']
        }
    )

if __name__ == "__main__":
    uvicorn.run(
        f"{__name__}:app",
        host="0.0.0.0",
        port=int(EXPOSE_PORT),
        workers=1,
        factory=False,
        reload=RELOAD_ENABLE,
    )
