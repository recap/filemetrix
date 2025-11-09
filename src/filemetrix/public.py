import json
import logging
import os
from datetime import datetime, date

import datahugger
import requests
import xml.etree.ElementTree as ET
import asyncio
import time
import os


from urllib.parse import unquote

from datahugger import RepositoryNotSupportedError
from datahugger.utils import get_datapublisher_from_doi, get_re3data_repositories


import asyncio
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import Response

from src.filemetrix import onedata_hugger
from src.filemetrix.commons import app_settings
from src.filemetrix.db import RepositoryModel, insert_repo, get_repo_by_id, get_repo_by_prefix_and_url, get_all_repos, \
    HarvestStatus, get_dataset_count_grouped_by_publication_month, get_file_metadata_count_grouped_by_repo, get_dataset_count_grouped_by_repo
from src.filemetrix.oai_harvester_client import OaiHarvesterClient
from src.filemetrix.db import get_file_metadata_count_by_repo_id
from src.filemetrix.db import get_dataset_count
from src.filemetrix.db import get_file_metadata_count
from src.filemetrix.db import get_dataset_count_by_repo_id
from src.filemetrix.db import get_dataset_count_by_repo_id_and_status

# Create an API router instance
router = APIRouter()


def serialize(obj):
    data = obj.model_dump()
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    return data

@router.get("/repo/{id}", tags=["Public"])
async def get_repo_by_id_public(id: int):
    repo = get_repo_by_id(id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")
    return JSONResponse(status_code=200, content=serialize(repo))

@router.get("/harvest/metadata_prefix/url:path", tags=["Public"])
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

@router.get("/repos", tags=["Public"])
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


@router.get("/dataset/count", tags=["Public"])
async def dataset_count():
    count = get_dataset_count()
    return JSONResponse(
        status_code=200,
        content={"total-datasets": count, "number-of-repositories": len(get_all_repos())}
    )



@router.get("/file-metadata/count", tags=["Public"])
async def file_metadata_count():
    count = get_file_metadata_count()
    return JSONResponse(
        status_code=200,
        content={"total-file-metadata": count, "number-of-repositories": len(get_all_repos())}
    )



@router.get("/dataset/count/{repo_id}", tags=["Public"])
async def dataset_count_by_repo_id(repo_id: int):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")

    count = get_dataset_count_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content={"repo-name": repo.name, "dataset-count": count}
    )



@router.get("/file-metadata/count/{repo_id}", tags=["Public"])
async def file_metadata_count_by_repo_id(repo_id: int):
    repo = get_repo_by_id(repo_id)
    if not repo:
        return HTTPException(status_code=404, detail="Repository not found.")

    count = get_file_metadata_count_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content={"file-metadata": count, "repo-name": repo.name}
    )


@router.get("/dataset/count/{repo_id}/status/{harvest_status}", tags=["Public"])
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

from src.filemetrix.db import get_dataset_count_by_repo_id_and_fm_status

@router.get("/dataset/count/{repo_id}/file-metadata/{harvest_status}", tags=["Public"])
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

from src.filemetrix.db import get_file_metadata_count_grouped_by_mime_type

@router.get("/file-metadata/count/grouped/mime_type", tags=["Public"])
async def file_metadata_count_grouped_by_mime_type():
    result = get_file_metadata_count_grouped_by_mime_type()
    return JSONResponse(
        status_code=200,
        content=result
    )

from src.filemetrix.db import get_file_metadata_count_grouped_by_mime_type_by_repo_id

@router.get("/file-metadata/count/grouped/mime_type/{repo_id}", tags=["Public"])
async def file_metadata_count_grouped_by_mime_type_by_repo_id(repo_id: int):
    result = get_file_metadata_count_grouped_by_mime_type_by_repo_id(repo_id)
    return JSONResponse(
        status_code=200,
        content=result
    )

from src.filemetrix.db import get_total_file_size_by_repo_id

def format_size(size_bytes: int) -> str:
    units = [("T", 1024 ** 4), ("Gb", 1024 ** 3), ("Mb", 1024 ** 2), ("Kb", 1024), ("byte", 1)]
    parts = []
    for name, count in units:
        value = size_bytes // count
        if value:
            parts.append(f"{value} {name}")
            size_bytes %= count
    return " and ".join(parts) if parts else "0 byte"

@router.get("/file-metadata/total-size/{repo_id}", tags=["Public"])
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

@router.get("/dataset/count/grouped-by-publication/month/{repo_id}", tags=["Public"])
async def dataset_count_grouped_by_month(repo_id: int):
    result = get_dataset_count_grouped_by_publication_month(repo_id)
    return JSONResponse(
        status_code=200,
        content=result
    )

@router.get("/dataset/count/grouped/repo", tags=["Public"])
async def dataset_count_grouped_by_repo():
    result = get_dataset_count_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )

@router.get("/file-metadata/count/grouped/repo", tags=["Public"])
async def file_metadata_count_grouped_by_repo():
    result = get_file_metadata_count_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )


from src.filemetrix.db import get_total_file_size_grouped_by_repo

from src.filemetrix.db import get_total_file_size_grouped_by_repo

@router.get("/file-metadata/total-size/grouped/repo", tags=["Public"])
async def file_metadata_total_size_grouped_by_repo():
    result = get_total_file_size_grouped_by_repo()
    return JSONResponse(
        status_code=200,
        content=result
    )

from src.filemetrix.db import get_repo_by_dataset_pid



@router.get("/repositories", tags=["Public - PID Fetcher"])
async def repositories():
    """
    Retrieve a list of available repositories from re3data, cached daily.
    """
    cache_file = "repositories_cache.json"
    today = date.today().isoformat()

    # Try to load from cache
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("date") == today and "repos" in cache:
                return JSONResponse(status_code=200, content=cache["repos"])
        except Exception as e:
            logging.warning(f"Failed to read cache: {e}")

    # Fetch from remote and update cache
    try:
        data_repos = requests.get("https://www.re3data.org/api/v1/repositories")
        data_repos.raise_for_status()
        tree = ET.fromstring(data_repos.content)
        repos = [{elem.tag: elem.text for elem in node if elem.tag != "link"} for node in tree]

        # Save to cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({"date": today, "repos": repos}, f)
        except Exception as e:
            logging.warning(f"Failed to write cache: {e}")

        return JSONResponse(status_code=200, content=repos)
    except Exception as e:
        logging.error(f"Error fetching repositories: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch repositories")


@router.get("/repository-collections/{r3id}", tags=["Public - PID Fetcher"],
    summary="Retrieve details of a specific repository",
    description="Fetches detailed information about a repository from re3data based on the provided repository ID.")
async def repository_details(r3id: str):
    """
    Retrieve details of a specific repository from re3data.
    """
    cache_file = "repositories_cache.json"
    try:
        # Try to load from cache
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            repos = cache.get("repos", [])
            repo = next((r for r in repos if r.get("id") == r3id), None)
            if not repo:
                return HTTPException(status_code=404, detail=f"Repository '{r3id}' not found", headers={})

        response = requests.get(f"https://www.re3data.org/api/v1/repository/{r3id}")
        response.raise_for_status()
        url = "http://transformer.labs.dansdemo.nl/transform/r3data-xml-to-json.xsl"
        headers = {
            "Content-Type": "application/xml",
            "Authorization": f"Bearer {app_settings.METADATA_TRANSFORMER_SERVICE_API_KEY}"
        }
        transformer_response = requests.post(url, headers=headers, data=response.text)
        result = json.loads(transformer_response.text)
        r = result["result"]
        rst = json.loads(r)
        oai = rst["repository"].get("oai", {})
        set_specs = []
        if oai:
            oai_url = f"{oai.split('?')[0]}?verb=ListSets"
            logging.info(oai_url)
            response = requests.get(oai_url)
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch OAI data")
            logging.info(f"Fetched OAI data from {oai_url}")
            xml_data = response.text
            ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
            root = ET.fromstring(xml_data)
            set_specs = [elem.text for elem in root.findall('.//oai:setSpec', ns)]

        return JSONResponse(status_code=200, content=set_specs)
    except requests.HTTPError as e:
        logging.error(f"HTTP error fetching repository details: {e}")
        return HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        logging.error(f"Error fetching repository details: {e}")
        return HTTPException(status_code=500, detail="Failed to fetch repository details")


@router.get("/repository-info/{pid:path}",tags=["Public - PID Fetcher"],
    summary="Retrieve repository information for a given PID",
    description="Fetches repository information from re3data based on the provided persistent identifier (PID).")
async def retrieve_repo_info(pid: str):

    decoded_pid = unquote(pid).replace("doi:", "")
    try:
        publisher = get_datapublisher_from_doi(decoded_pid)
        if not publisher:
            raise HTTPException(status_code=404, detail="Publisher not found for PID")
        data_repos = get_re3data_repositories()
        for repo in data_repos:
            if publisher.lower() == repo["name"].lower():
                repo_data =requests.get(f"https://www.re3data.org/api/v1/repository/{repo['id']}")
                url = "http://transformer.labs.dansdemo.nl/transform/r3data-xml-to-json.xsl"
                headers = {
                    "Content-Type": "application/xml",
                    "Authorization": f"Bearer {app_settings.METADATA_TRANSFORMER_SERVICE_API_KEY}"
                }
                transformer_response = requests.post(url, headers=headers, data=repo_data.text)
                result = json.loads(transformer_response.text)
                r = result["result"]
                rst = json.loads(r)
                oai = rst["repository"].get("oai", {})
                if oai:
                    oai_url = f"{oai.split('?')[0]}?verb=GetRecord&identifier={pid}&metadataPrefix=oai_dc"
                    logging.info(oai_url)
                    response = requests.get(oai_url)
                    if response.status_code != 200:
                        raise HTTPException(status_code=response.status_code, detail="Failed to fetch OAI data")

                    logging.info(f"Fetched OAI data from {oai_url}")
                    xml_data = response.text
                    ns = {  'dc': 'http://purl.org/dc/elements/1.1/',
                            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
                            'oai': 'http://www.openarchives.org/OAI/2.0/'}
                    root = ET.fromstring(xml_data)
                    title = root.find('.//dc:title', ns)
                    identifier = root.find('.//dc:identifier', ns)
                    rst["title"] = title.text if title is not None else None
                    rst["identifier"] = identifier.text if identifier is not None else None

                    set_specs = [elem.text for elem in root.findall('.//oai:header/oai:setSpec', ns)]

                    rst["collections"] = set_specs
                else:
                    logging.warning("No OAI information found in repository data")
                    rst["title"] = ""
                    rst["identifier"] = decoded_pid
                    rst["collections"] = []

                return Response(content=json.dumps(rst), media_type="application/json")
        raise HTTPException(status_code=404, detail="Repository not found in re3data")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/extensions/{pid:path}",tags=["Public - PID Fetcher"],)
async def get_extensions(pid: str):
    decoded_pid = unquote(pid).replace("doi:", "")
    try:
        metadata = await asyncio.to_thread(datahugger.info, decoded_pid, {"type": "file"})
        extensions = set()
        for file in metadata.files:
            raw_metadata = file.get('raw_metadata', {})
            content_type = raw_metadata.get('contentType')
            if content_type:
                ext = content_type.split('/')[-1]
                extensions.add(ext)
        return JSONResponse(status_code=200, content={"extensions": list(extensions)})
    except RepositoryNotSupportedError as e:
        logging.error(f"Repository not supported: {e}")
        raise HTTPException(status_code=400, detail="Repository not supported")
    except Exception as e:
        logging.error(f"Error fetching metadata: {e}")
        raise HTTPException(status_code=500, detail="Error fetching metadata")
@router.get(
    "/{pid:path}",
    response_class=JSONResponse,
    summary="Fetch metadata files for a given PID",
    description="Retrieves metadata files for the provided persistent identifier (PID). Optionally allows downloading the files.",
    tags=["Public - PID Fetcher"])
async def get_pid(pid: str):
    start_time = time.perf_counter()
    logging.info("get doi")
    decoded_doi = unquote(pid)
    print(f"Received DOI: {decoded_doi}")
    logging.info(f"Received DOI: {decoded_doi}")
    try:
        metadata = await asyncio.to_thread(datahugger.info, decoded_doi, {"type": "file"})
        # metadata =datahugger.info(decoded_doi)
    except RepositoryNotSupportedError as e:
        # fall-back and try to resolve the identifier as Onedata dataset
        metadata = onedata_hugger.info(decoded_doi)
        if not metadata:
            duration = time.perf_counter() - start_time
            if duration > 30:
                logging.warning(f"Request duration exceeded 30 seconds: {duration:.4f} seconds")
                print(f"WARNING: Request duration exceeded 30 seconds: {duration:.4f} seconds")
            logging.error(f"Repository not supported: {e}")
            logging.info(f"Request duration: {duration:.4f} seconds")
            return JSONResponse(
                status_code=400,
                content={"error": "Repository not supported", "message": str(e), "duration": duration}
            )
    except Exception as e:
        duration = time.perf_counter() - start_time
        if duration > 30:
            logging.warning(f"Request duration exceeded 30 seconds: {duration:.4f} seconds")
            print(f"WARNING: Request duration exceeded 30 seconds: {duration:.4f} seconds")
        logging.error(f"Error fetching metadata: {e}")
        logging.info(f"Request duration: {duration:.4f} seconds")
        return JSONResponse(
            status_code=500,
            content={"error": "Repository not supported", "message": str(e), "duration": duration}
        )
    logging.info(f"Return metadata files for {decoded_doi}")
    duration = time.perf_counter() - start_time
    if duration > 30:
        logging.warning(f"Request duration exceeded 30 seconds: {duration:.4f} seconds")
        print(f"WARNING: Request duration exceeded 30 seconds: {duration:.4f} seconds")
    logging.info(f"Request duration: {duration:.4f} seconds")
    print(f"Request duration: {duration:.4f} seconds")
    return {"files": metadata.files}
