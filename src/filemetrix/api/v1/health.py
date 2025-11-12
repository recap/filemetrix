from fastapi import APIRouter, Response
from sqlalchemy import text
from sqlmodel import Session
from src.filemetrix.infra.db import engine

router = APIRouter()

@router.get("/health", include_in_schema=True, summary="Service health", description="Basic liveness/readiness check. Returns 200 when DB is reachable.")
async def health_check(response: Response):
    """Check DB connectivity by running a lightweight SELECT 1."""
    try:
        with Session(engine) as session:
            session.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        response.status_code = 503
        return {"status": "unhealthy", "reason": str(e)}
