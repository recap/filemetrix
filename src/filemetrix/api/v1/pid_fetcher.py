import asyncio
import json
import logging
import time
import xml.etree.ElementTree as ET
from urllib.parse import unquote

import datahugger
import requests
from datahugger import RepositoryNotSupportedError
from datahugger.utils import get_datapublisher_from_doi, get_re3data_repositories
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import Response

from src.filemetrix.infra.commons import app_settings, API_PREFIX
from src.filemetrix.services import onedata_hugger

router = APIRouter(prefix=API_PREFIX)

# All discovery-related endpoints:
# - /repositories (re3data cache)
# - /repository-collections/{r3id}
# - /repository-info/{pid:path}
# - /extensions/{pid:path}
# - /{pid:path} (main PID fetcher)


# This endpoint retrieves repository information for a given PID
# and fetches OAI data if available from the repository. It uses re3data
# to find the repository based on the publisher extracted from the PID (DOI).
# It then transforms the re3data XML response to JSON and attempts to fetch OAI metadata.
# If OAI data is found, it extracts the title, identifier, and collections. 
# If no OAI data is available, it returns basic repository information.
@router.get("/repository-info/{pid:path}",tags=["PID Fetcher"],
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


@router.get("/extensions/{pid:path}",tags=["PID Fetcher"],)
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
    tags=["PID Fetcher"])
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
