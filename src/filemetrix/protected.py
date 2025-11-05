import logging

import asyncio
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from src.filemetrix.commons import app_settings, send_gmail
from src.filemetrix.db import RepositoryModel, insert_repo, get_repo_by_id, get_repo_by_prefix_and_url, HarvestStatus, \
    delete_file_metadata_by_dataset_pid
from src.filemetrix.oai_harvester_client import OaiHarvesterClient

# Create an API router instance
router = APIRouter()


@router.post("/add-repo", tags=["Protected"])
async def add_repo(
    request: Request):# Validate Bearer token
    try:
        payload = await request.json()
        inserted_count = 0

        repos = payload if isinstance(payload, list) else [payload]
        for repo_data in repos:
            logging.info(f"Processing repository: {repo_data}")
            repo = RepositoryModel.model_validate(repo_data)
            logging.info(f"Validated payload: {repo}")
            result = insert_repo(repo)

            if not isinstance(result, int):
                logging.error(f"Failed to add repository: {result}")
                return HTTPException(
                    status_code=400,
                    detail=result,
                    headers={"X-Error": "Repository addition failed"},
                )
            inserted_count += 1

        return JSONResponse(
            status_code=200,
            content={
                "number-inserted-repo": inserted_count,
                "message": "Repository added successfully",
                "data": payload,
            },
        )
    except Exception as e:
        logging.error(f"Error processing payload: {e}")
        return HTTPException(
            status_code=400, detail="Invalid JSON payload or processing error"
        )
from fastapi import BackgroundTasks

@router.post("/harvest/{metadata_prefix}/{url:path}", tags=["Protected"])
@router.post("/harvest/{repo_id}", tags=["Protected"])
async def pid_harvest(
    request: Request,
    background_tasks: BackgroundTasks,
    repo_id: int = None,
    metadata_prefix: str = None,
    url: str = None
):
    # Select repository based on provided parameters
    if repo_id is not None:
        repo = get_repo_by_id(repo_id)
    elif metadata_prefix and url:
        repo = get_repo_by_prefix_and_url(metadata_prefix, url)
    else:
        return HTTPException(
            status_code=400,
            detail="Must provide either 'id' or both 'metadata_prefix' and 'url'."
        )

    if not repo:
        return HTTPException(
            status_code=404,
            detail="Repository not found.",
            headers={"X-Error": "Repository not found."}
        )

    if repo.harvest_ds_status == HarvestStatus.IN_PROGRESS:
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Repository '{repo.name}' harvest is already in progress. Please wait until it is completed."}
        )

    logging.info(f"Processing harvest PID, repo name: {repo.name}")
    subject = f"Dataset harvest for repository {repo.name} started"
    body = f"Dataset harvest for repository {repo.name} has started. Please check the status later."
    send_gmail(subject, body)
    def run_harvest_sync():
        async def run_harvest():
            harvester = OaiHarvesterClient(repo)
            await harvester.harvest_identifiers()
        asyncio.run(run_harvest())

    background_tasks.add_task(run_harvest_sync)

    return JSONResponse(
        status_code=200,
        content={"message": "Dataset harvest in progress", "repository": repo.name}
    )
from fastapi import BackgroundTasks

@router.post("/harvest-filemetadata/{metadata_prefix}/{url:path}", tags=["Protected"])
@router.post("/harvest-filemetadata/{repo_id}", tags=["Protected"])
async def filemetadata_harvest(
    request: Request,
    background_tasks: BackgroundTasks,
    repo_id: int = None,
    metadata_prefix: str = None,
    url: str = None
):

    if repo_id is not None:
        repo = get_repo_by_id(repo_id)
    elif metadata_prefix and url:
        repo = get_repo_by_prefix_and_url(metadata_prefix, url)
    else:
        return HTTPException(
            status_code=400,
            detail="Must provide either 'id' or both 'metadata_prefix' and 'url'."
        )

    if not repo:
        return HTTPException(
            status_code=404,
            detail="Repository not found.",
            headers={"X-Error": "Repository not found."}
        )

    if repo.harvest_ds_status != "completed":
        return HTTPException(
            status_code=400,
            detail="Repository harvest must be completed before harvest file metadata.",
            headers={"X-Error": "Repository harvest not completed."}
        )
    logging.info(f"Processing filemetadata: {repo.name}")
    print(f"Processing filemetadata: {repo.name}")
    subject = f"File metadata harvest for repository {repo.name} started"
    body = f"File metadata harvest for repository {repo.name} has started. Please check the status later."
    send_gmail(subject, body)
    def run_harvest_sync():
        async def run_harvest():
            logging.info(f"Starting file metadata harvest for repository: {repo.name}")
            print(f"Starting file metadata harvest for repository: {repo.name}")
            harvester = OaiHarvesterClient(repo)
            # tasks = [
            #     harvester.harvest_files(dataset.pid, app_settings.PID_FETCHER_URL)
            #     for dataset in repo.datasets
            #     if not dataset.harvest_fm_status == HarvestStatus.COMPLETED
            # ]
            tasks = []
            for dataset in repo.datasets:
                if dataset.harvest_fm_status == HarvestStatus.IN_PROGRESS:
                    delete_file_metadata_by_dataset_pid(dataset.pid)# TODO: Need to review this
                if dataset.harvest_fm_status != HarvestStatus.COMPLETED:
                    tasks.append(harvester.harvest_files(dataset.repo_id, dataset.pid, app_settings.PID_FETCHER_URL))

            await asyncio.gather(*tasks)
            logging.info(f"File metadata harvest completed for repository: {repo.name}")
            print(f"File metadata harvest completed for repository: {repo.name}")
            subject = f"File metadata harvest for repository {repo.name} completed"
            body = f"File metadata harvest for repository {repo.name} has completed successfully."
            send_gmail(subject, body)
        asyncio.run(run_harvest())

    background_tasks.add_task(run_harvest_sync)

    return JSONResponse(
        status_code=200,
        content={"message": "File metadata harvest in progress", "repository": repo.name}
    )
