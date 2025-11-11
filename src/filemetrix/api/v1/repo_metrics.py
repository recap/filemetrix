from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from src.filemetrix.infra.commons import API_PREFIX
from src.filemetrix.infra.db import get_repo_by_id, get_repo_by_prefix_and_url, get_all_repos, get_dataset_count, \
    get_file_metadata_count, get_dataset_count_by_repo_id, get_file_metadata_count_by_repo_id, \
    get_dataset_count_by_repo_id_and_status, HarvestStatus, get_dataset_count_by_repo_id_and_fm_status, \
    get_file_metadata_count_grouped_by_mime_type, get_file_metadata_count_grouped_by_mime_type_by_repo_id, \
    get_total_file_size_by_repo_id, get_dataset_count_grouped_by_publication_month, get_dataset_count_grouped_by_repo, \
    get_file_metadata_count_grouped_by_repo, get_total_file_size_grouped_by_repo

router = APIRouter(prefix=API_PREFIX)

def serialize(obj):
    data = obj.model_dump()
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    return data

@router.get("/repo/{id}", tags=["Repo Metrics"])
async def get_repo_by_id_public(id: int):
    repo = get_repo_by_id(id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")
    return JSONResponse(status_code=200, content=serialize(repo))

@router.get("/harvest/metadata_prefix/url:path", tags=["Repo Metrics"])
async def harvest_metadata(metadata_prefix: str = None, url: str = None):
    if not metadata_prefix or not url:
        return HTTPException(
            status_code=400,
            detail="Both 'metadata_prefix' and 'url' must be provided."
        )
    repose = get_repo_by_prefix_and_url(metadata_prefix, url)
    return JSONResponse(
        status_code=200,
        content=repose.model_dump()
    )

@router.get("/repos", tags=["Repo Metrics"])
async def get_repos_public():
    repos = get_all_repos()
    # Use model_dump(mode="json") if available, else fallback to manual conversion
    def serialize(obj):
        data = obj.model_dump()
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = v.isoformat()
        return data
    return JSONResponse(
        status_code=200,
        content=[serialize(repo) for repo in repos]
    )


@router.get("/dataset/count", tags=["Repo Metrics"])
async def dataset_count():
    count = get_dataset_count()
    return JSONResponse(
        status_code=200,
        content={"total-datasets": count, "number-of-repositories": len(get_all_repos())}
    )



@router.get("/file-metadata/count", tags=["Repo Metrics"])
async def file_metadata_count():
    count = get_file_metadata_count()
    return JSONResponse(
        status_code=200,
        content={"total-file-metadata": count, "number-of-repositories": len(get_all_repos())}
    )



@router.get("/dataset/count/{repo_id}", tags=["Repo Metrics"])
async def dataset_count_by_repo_id(repo_id: int):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")

    count = get_dataset_count_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content={"repo-name": repo.name, "dataset-count": count}
    )



@router.get("/file-metadata/count/{repo_id}", tags=["Repo Metrics"])
async def file_metadata_count_by_repo_id(repo_id: int):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")

    count = get_file_metadata_count_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content={"file-metadata": count, "repo-name": repo.name}
    )


@router.get("/dataset/count/{repo_id}/status/{harvest_status}", tags=["Repo Metrics"])
async def dataset_count_by_repo_id_and_status(repo_id: int, harvest_status: HarvestStatus):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")
    count = get_dataset_count_by_repo_id_and_status(repo_id, harvest_status)
    if count == 0 and harvest_status == HarvestStatus.IN_PROGRESS:
        return JSONResponse(
            status_code=200,
            content={"message": f"Repository '{repo.name}' has completed harvest."}

        )

    return JSONResponse(
        status_code=200,
        content={"datasets": count, "repo-name": repo.name}
    )


@router.get("/dataset/count/{repo_id}/file-metadata/{harvest_status}", tags=["Repo Metrics"])
async def dataset_count_by_repo_id_and_fm_status(repo_id: int, harvest_status: HarvestStatus):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")
    count = get_dataset_count_by_repo_id_and_fm_status(repo_id, harvest_status)
    if count == 0:
        return JSONResponse(
            status_code=200,
            content={"message": f"Repository '{repo.name}' has no dataset metadata with file metadata status '{harvest_status.value}'."}
        )
    return JSONResponse(
        status_code=200,
        content={"dataset-count": count, "repo-name": repo.name}
    )

@router.get("/file-metadata/count/grouped/mime_type", tags=["Repo Metrics"])
async def file_metadata_count_grouped_by_mime_type():
    result = get_file_metadata_count_grouped_by_mime_type()
    return JSONResponse(
        status_code=200,
        content=result
    )


@router.get("/file-metadata/count/grouped/mime_type/{repo_id}", tags=["Repo Metrics"])
async def file_metadata_count_grouped_by_mime_type_by_repo_id(repo_id: int):
    result = get_file_metadata_count_grouped_by_mime_type_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content=result
    )


def format_size(size_bytes: int) -> str:
    units = [("T", 1024 ** 4), ("Gb", 1024 ** 3), ("Mb", 1024 ** 2), ("Kb", 1024), ("byte", 1)]
    parts = []
    for name, count in units:
        value = size_bytes // count
        if value:
            parts.append(f"{value} {name}")
            size_bytes %= count
    return " and ".join(parts) if parts else "0 byte"

@router.get("/file-metadata/total-size/{repo_id}", tags=["Repo Metrics"])
async def file_metadata_total_size_by_repo_id(repo_id: int):
    total_size = get_total_file_size_by_repo_id(repo_id)
    total_size_int = int(total_size)
    repo = get_repo_by_id(repo_id)
    return JSONResponse(
        status_code=200,
        content={
            "repo-id": repo_id,
            "repo-name": repo.name if repo else "Unknown",
            "total-size": total_size_int,
            "total-size-in-friendly": format_size(total_size_int)
        }
    )

@router.get("/dataset/count/grouped-by-publication/month/{repo_id}", tags=["Repo Metrics"])
async def dataset_count_grouped_by_month(repo_id: int):
    result = get_dataset_count_grouped_by_publication_month(repo_id)
    return JSONResponse(
        status_code=200,
        content=result
    )

@router.get("/dataset/count/grouped/repo", tags=["Repo Metrics"])
async def dataset_count_grouped_by_repo():
    result = get_dataset_count_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )

@router.get("/file-metadata/count/grouped/repo", tags=["Repo Metrics"])
async def file_metadata_count_grouped_by_repo():
    result = get_file_metadata_count_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )


@router.get("/file-metadata/total-size/grouped/repo", tags=["Repo Metrics"])
async def file_metadata_total_size_grouped_by_repo():
    result = get_total_file_size_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )



