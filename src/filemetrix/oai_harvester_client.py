from __future__ import annotations

import pickle
import json
import logging
from datetime import datetime
import requests

from sickle import Sickle

from src.filemetrix.commons import send_gmail, app_settings
from src.filemetrix.db import FileMetaDataModel, DatasetModel, insert_dataset, get_repo_by_prefix_and_url, \
    update_repository_harvest_info, RepositoryModel, insert_file_metadata, \
    update_dataset_harvest_fm_end_completed, update_dataset_harvest_fm_start_in_progress, dataset_exists


def transform_input(transformer_url, str_tobe_transformed):
    pass

from datetime import datetime
import time
def parse_datestamp(datestamp: str) -> datetime | None:
    if not datestamp:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(datestamp, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unknown datestamp format: {datestamp}")

class OaiHarvesterClient():

    def __init__(self, repo: RepositoryModel):
        self.oai_url = repo.url
        self.metadataPrefix = repo.metadata_prefix
        self.repo_id = repo.id

    async def harvest_identifiers(self) -> int:
        logging.info(f'harvest of {self.oai_url} and metadataPrefix {self.metadataPrefix}')
        update_repository_harvest_info(self.repo_id, harvest_start=datetime.now(),
                                          harvest_status="in_progress")

        sickle = Sickle(self.oai_url)
        records = sickle.ListRecords(metadataPrefix=self.metadataPrefix)

        total_processed, total_skipped, total_inserted = 0, 0, 0

        for record in records:
            total_processed += 1
            if record.header.deleted:
                total_skipped += 1
                logging.warning(f"Dataset - Skipping deleted record: {record.header.identifier}")
                continue
            if not record.header.identifier:
                logging.error(f"Skipping empty identifier record: {record.header.identifier}")
                continue

            total_inserted += 1
            pid_protocol = "doi"
            if record.header.identifier.startswith('doi'):
                record.header.identifier = record.header.identifier.replace('doi:', '')
            elif record.header.identifier.startswith('hdl'):
                record.header.identifier = record.header.identifier.replace('hdl:', '')
                pid_protocol = "hdl"
            elif record.header.identifier.startswith('ark'):
                record.header.identifier = record.header.identifier.replace('ark:/', '')
                pid_protocol = "ark"

            a = record.metadata.get("date", None)
            if a is not None:
                if isinstance(a, list):
                    record.metadata["date"] = a[0]

            if dataset_exists(record.header.identifier, self.repo_id):
                total_skipped += 1
                logging.warning(f"Skipping --- Dataset already exists for PID: {record.header.identifier} in repo {self.repo_id}")
                print("Skipping --- Dataset already exists for PID: " + record.header.identifier + " in repo " + str(self.repo_id))
                # TODO: check if the harvest files are already in progress
                continue

            new_dataset = DatasetModel(
                repo_id=self.repo_id,
                pid=record.header.identifier,
                pid_protocol=pid_protocol,
                timestamp=parse_datestamp(record.header.datestamp),
                publication_date=parse_datestamp(record.metadata.get("date", None)),
                # publisher= "#".join(record.metadata.get('publisher', '')),
                # language=",".join(record.metadata.get('language', '')),
            )
            # print(new_dataset.model_dump())
            insert_dataset(new_dataset)

        print(f"Total Dataset records processed: {total_processed}")
        print(f"Total Dataset records skipped: {total_skipped}")
        print(f"Total Dataset records inserted: {total_inserted}")
        logging.info(f"Total Dataset records processed: {total_processed}")
        logging.info(f"Total Dataset records skipped: {total_skipped}")
        logging.info(f"Total Dataset records inserted: {total_inserted}")

        update_repository_harvest_info(self.repo_id, harvest_end=datetime.now(), harvest_status="completed")
        subject = "FileMetrix Harvest Completed"
        body = (f"Harvest completed for repository {self.oai_url} with metadataPrefix {self.metadataPrefix}.\n"
                f"Total Dataset records processed: {total_processed}\nTotal Dataset records skipped: {total_skipped}\n"
                f"Total Dataset records inserted: {total_inserted}")

        send_gmail(subject, body)
        return total_processed

    async def harvest_files(self, repo_id: int, pid: str, pid_fetcher_url: str = "https://pid-fetcher.labs.dansdemo.nl/") -> int| None:
        start_time = time.time()
        logging.info(f'Starting file harvest for {pid} from repository {repo_id}')
        print(f'Starting file harvest for {pid} from repository {repo_id}')
        update_dataset_harvest_fm_start_in_progress(pid)
        try:
            #The default for the timeout parameter in requests.get (from the requests library) is None,
            # which means it will wait indefinitely for a response unless a timeout is explicitly set.
            # files_metadata = requests.get(f"{pid_fetcher_url}{pid}", timeout=1800)  # 30 minutes timeout
            files_metadata = requests.get(f"{pid_fetcher_url}{pid}")
        except requests.exceptions.Timeout:
            logging.error(f"Request for {pid} timed out.")
            subject = "FileMetrix Harvest Timeout"
            body = f"Request for {pid} timed out while fetching metadata files from repository {repo_id}."
            send_gmail(subject, body)
            return None

        if files_metadata.status_code != 200:
            logging.error(f"Failed to fetch metadata for {pid}: {files_metadata.status_code}")
            return None

        logging.info(f"Fetched metadata for {pid} from repository {repo_id}, response status_code: {files_metadata.status_code}")
        print(f'Fetched metadata for {pid} from repository {repo_id}, response status_code: {files_metadata.status_code}')
        # print(files_metadata.json())
        total_processed, total_skipped, total_inserted = 0, 0, 0
        # print(json.dumps(files_metadata.json()))
        for fm in files_metadata.json().get('files', []):
            total_processed += 1
            # Logic for skipping records can increment total_skipped if needed
            total_inserted += 1
            fmdm = FileMetaDataModel(
                name=fm['name'],
                link=fm['link'],
                size=fm['size'],
                mime_type=fm['raw_metadata']['contentType'],
                checksum_value=fm['raw_metadata']['checksum']['value'],
                checksum_type=fm['raw_metadata']['checksum']['type'],
                access_request=fm['raw_metadata']['fileAccessRequest'],
                publication_date=fm['raw_metadata']['publicationDate'],
                embargo=fm['raw_metadata']['embargo']['dateAvailable'] if 'embargo' in fm['raw_metadata'] else None,
                file_pid=None,
                dataset_pid=pid
            )
            logging.info("Inserting File Metadata: " + str(fmdm.link))
            print("Inserting File Metadata: " + str(fmdm.link))

            insert_file_metadata(fmdm)

        print("Total File Metadata records processed:", total_processed)
        print("Total File Metadata records skipped:", total_skipped)
        print("Total File Metadata records inserted:", total_inserted)
        logging.info(f"Total File Metadata records processed: {total_processed}")
        logging.info(f"Total File Metadata records skipped: {total_skipped}")
        logging.info(f"Total File Metadata records inserted: {total_inserted}")
        print(f"Completed harvest files for dataset:{ pid}")
        logging.info(f"Completed harvest files for dataset:{ pid}")
        duration = time.time() - start_time
        logging.info(f"harvest_files for {pid} took {duration:.2f} seconds")
        print(f"harvest_files for {pid} took {duration:.2f} seconds")
        if duration > 60:
            msg = f"harvest_files for {pid} took {duration:.2f} seconds, which exceeds 60 seconds."
            print(msg)
            logging.warning(msg)
        update_dataset_harvest_fm_end_completed(pid)
        return total_processed


    async def harvest_identifiers2(self, from_date=None, until_date=None, saved_token_file=f'{app_settings.PKL_TOKEN_FILE}/token.pkl'):
        sickle = Sickle(self.oai_url)
        try:
            # Try to load saved resumptionToken
            resumption_token = None
            try:
                with open(saved_token_file, 'rb') as f:
                    resumption_token = pickle.load(f)
                    logging.info(f"Resuming with token: {saved_token_file}")
                    print(f"Resuming with token: {resumption_token}")
            except FileNotFoundError:
                pass  # Start from beginning

            if resumption_token:
                # Resume from saved token
                records = sickle.ListIdentifiers(resumptionToken=resumption_token)
            else:
                # Start new harvest
                records = sickle.ListIdentifiers(
                    self.metadataPrefix,
                    from_=from_date,
                    until=until_date
                )
            total_processed, total_skipped, total_inserted = 0, 0, 0

            for header in records:
                # Process header
                print(f"Identifier: {header.identifier}")
                logging.info(f"Identifier: {header.identifier}")
                # Save current token after each step
                if records.resumption_token is not None:
                    with open(saved_token_file, 'wb') as f:
                        pickle.dump(records.resumption_token.token, f)

            # Cleanup token if finished
            with open(saved_token_file, 'wb') as f:
                pickle.dump(None, f)

        except Exception as e:
            print(f"Error: {e}")
            logging.error(f"Error during harvest_identifiers2: {e}")
