import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import date

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from src.filemetrix.infra.commons import app_settings, API_PREFIX

router = APIRouter(prefix=API_PREFIX)


@router.get("/repositories", tags=["Repo Discovery"],)
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

@router.get("/repository-collections/{r3id}", tags=["PID Fetcher"],
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
