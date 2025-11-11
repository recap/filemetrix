
import logging
import re
import requests
import traceback

from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import (
    ConnectionError,
    Timeout,
    RequestException
)
from types import SimpleNamespace
from urllib.parse import urlparse


DOI_RESOLVER_ADDRESS = "https://doi.org"
MAX_REDIRECTS = 100
REQUEST_TIMEOUT = 10.0
MAX_THREADS = 8


def info(identifier: str):
    try:
        log_info(f"Attempting to resolve an identifier as a Onedata dataset: {identifier}")
        metadata = info_unsafe(identifier, MAX_REDIRECTS)
        if not metadata:
            return None
        count = len(metadata.get("files"))
        log_info(f"Successfully resolved a Onedata dataset - {count} file(s): {identifier}")
        return SimpleNamespace(**metadata)
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"Error fetching Onedata dataset metadata: {e}\n{tb}")
        return None
        

def info_unsafe(identifier: str, max_redirects: int):
    if max_redirects < 1:
        log_error(
            f"Resigning after reaching max redirects ({MAX_REDIRECTS}) "
            f"with identifier: {identifier}")
        return None

    url = identifier_to_url(identifier)
    if not url:
        return None
    
    onezone_domain, share_id = identify_as_share_link(url)
    if onezone_domain and share_id:
        return gather_info_from_dataset(onezone_domain, share_id)
    
    redirect_url = peek_redirect(url)

    if not redirect_url:
        return False

    return info_unsafe(redirect_url, max_redirects - 1)
    

def gather_info_from_dataset(onezone_domain: str, share_id: str):
    data = call_rest_api(onezone_domain, f"/shares/{share_id}/public") 
    root_file_id = data.get("rootFileId")
    space_id = data.get("spaceId", "unknown")
    if data.get("fileType") == "DIR":
        return {
            "files": gather_file_infos_for_directory(onezone_domain, space_id, root_file_id)
        }
    elif data.get("fileType") == "REG":
        return {
            "files": [resolve_shared_file_info(onezone_domain, space_id, root_file_id)]
        }
    else:
        log_error(f"Unexpected Onedata share fileType: {data.get("fileType", "unknown")}")
        return None


def gather_file_infos_for_directory(
    onezone_domain: str, 
    space_id: str,
    file_id: str, 
    paging_token=None, 
    executor=None
):
    dir_listing_data = call_rest_api(
        onezone_domain, 
        f"/shares/data/{file_id}/children",
        body_json={
            "attributes": ["fileId", "name", "type", "size"],
            "token": paging_token
        },
        raise_for_status=False,
        failure_log_details="Cannot fetch files inside a shared directory. All nested files will be omitted."
    )
    if not dir_listing_data:
        return []
    
    children = dir_listing_data.get("children", [])
    results = []

    # create a shared executor at the root
    is_root_executor = executor is None
    if is_root_executor:
        executor = ThreadPoolExecutor(max_workers=MAX_THREADS)

    futures = []

    try:
        for child in children:
            child_id = child.get("fileId")

            if child.get("type") == "DIR":
                futures.append(
                    executor.submit(
                        gather_file_infos_for_directory,
                        onezone_domain,
                        space_id,
                        child_id,
                        None,
                        executor,
                    )
                )
            else:
                futures.append(
                    executor.submit(
                        build_file_info,
                        onezone_domain,
                        space_id,
                        child_id,
                        child.get("name"),
                        child.get("size"),
                    )
                )

        for f in as_completed(futures):
            try:
                result = f.result()
                if isinstance(result, list):
                    results.extend(result)  # directory result
                elif result:
                    results.append(result)  # single file info
            except Exception as e:
                tb = traceback.format_exc()
                log_error(f"Child task failed: {e}\n{tb}")

        if not dir_listing_data.get("isLast"):
            results.extend(
                gather_file_infos_for_directory(
                    onezone_domain,
                    space_id,
                    file_id,
                    paging_token=dir_listing_data.get("nextPageToken"),
                    executor=executor,
                )
            )

    finally:
        if is_root_executor:
            executor.shutdown(wait=True)

    return results


def resolve_shared_file_info(onezone_domain: str, space_id: str, file_id: str):
    attributes = call_rest_api(
        onezone_domain, 
        f"/shares/data/{file_id}",
        body_json={
            "attributes": ["fileId", "name", "size"]
        }
    )
    return build_file_info(
        onezone_domain, 
        space_id, 
        attributes.get("fileId"), 
        attributes.get("name"), 
        attributes.get("size")
    ) 


def build_file_info(onezone_domain: str, space_id: str, file_id: str, name: str, size: int):
    return  {
        "link": build_rest_api_uri(onezone_domain, file_id),
        "name": name,
        "size": size,
        "hash": None,
        "hash_type": "md5",
        "ro_crate_extensions": {
             "onedata:onezoneDomain": onezone_domain,
            "onedata:spaceId": space_id,
            "onedata:fileId": file_id,
            "onedata:publicAccess": True
        }
    }


def call_rest_api(
        onezone_domain: str, 
        path: str, 
        body_json=None, 
        raise_for_status=True,
        failure_log_details=None
    ):
    url = build_rest_api_uri(onezone_domain, path)
    response = requests.get(url, json=body_json)

    if not response.ok:
        if failure_log_details:
            details = f"{failure_log_details}\n> This failure was caused by:\n> " 
        else:
            details = ""

        log_warning(f"{details}Failed HTTP request for URL: {url}\n"
                    f"> Reponse body: {response.text if response.text else "<empty>"}\n"
        )

        if raise_for_status:
            response.raise_for_status()
        else:
            return None

    return response.json()


def build_rest_api_uri(onezone_domain: str, path: str):
    return f"https://{onezone_domain}/api/v3/onezone{path if path.startswith("/") else "/" + path}"


def identifier_to_url(identifier: str):
    identifier = identifier.strip()

    if identifier.lower().startswith(("http://", "https://")):
        return identifier
    
    if re.compile(r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$", re.IGNORECASE).match(identifier):
        return f"{DOI_RESOLVER_ADDRESS}/{identifier}"
    
    log_error(f"The identifier does not seem to be either a DOI or a URL: {identifier}")
    return None


def peek_redirect(url: str):
    try:
        response = requests.head(url, allow_redirects=False, timeout=REQUEST_TIMEOUT)
    except ConnectionError:
        log_warning(f"cannot resolve a redirection for url: {url}\n> Reason: connection error (host unreachable or DNS failure)")
        return None
    except Timeout:
        log_warning(f"cannot resolve a redirection for url: {url}\n> Reason: request timed out after {REQUEST_TIMEOUT}s")
        return None
    except RequestException as e:
        log_warning(f"cannot resolve a redirection for url: {url}\n> Reason: request failed ({type(e).__name__}: {e})")
        return None
    
    if 300 <= response.status_code < 400:
        redirect_url = response.headers.get("Location")
        if redirect_url:
            return redirect_url
        else:
            log_warning(f"cannot resolve a redirection for url: {url}\n> Reason: redirect response ({response.status_code}) without Location header")
            return None

    log_warning(f"cannot resolve a redirection for url: {url}\n> Reason: received a non-redirection HTTP code {response.status_code}")
    return None


# Heuristics to guess that this is a Onedata dataset - each published dataset is based 
# on a Onedata share, and all identifiers (PID / DOI) eventually redirect to a share URL.
# Onedata share URLs look like this: 
#     https://demo.onedata.org/share/7d66329c72b852dad4ea96186999bac2ch0687
def identify_as_share_link(url: str):
    if re.match(r"^https://[^/]+/share/[A-Za-z0-9]+$", url):
        onezone_domain = urlparse(url).netloc
        share_id = url.split("/")[-1]
        return onezone_domain, share_id
        
    return None, None


def log_info(msg: str):
    print(f"INFO:     {msg}")
    logging.info(msg)


def log_warning(msg: str):
    print(f"WARNING:  {msg}")
    logging.warning(msg)


def log_error(msg: str):
    print(f"ERROR:    {msg}")
    logging.error(msg)
