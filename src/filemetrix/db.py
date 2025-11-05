import logging
from enum import Enum
from typing import Optional, List
from datetime import datetime

import psycopg2
from sqlalchemy import Column, Integer, BigInteger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.schema import UniqueConstraint
from sqlmodel import SQLModel, Field, Relationship
from sqlmodel import create_engine, Session
from sqlalchemy.orm import selectinload

from src.filemetrix.commons import app_settings

dbname = "filemetrix"


password = app_settings.DB_PASSWORD
host = app_settings.DB_HOST
port = app_settings.DB_PORT
user = app_settings.DB_USER
DB_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DB_URL, echo=False)

def ensure_database_exists():

    # Connect to the default database
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(f'CREATE DATABASE "{dbname}"')
    conn.close()

class HarvestStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"

class RepositoryModel(SQLModel, table=True):
    __tablename__ = "repository"
    __table_args__ = (UniqueConstraint("url", "metadata_prefix", name="uix_url_metadata_prefix"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    metadata_prefix: str = Field(index=True)
    metadata_format: Optional[str] = None
    metadata_transformer_url: Optional[str] = None
    harvest_ds_start: Optional[datetime] = None
    harvest_ds_end: Optional[datetime] = None
    harvest_ds_status: Optional[HarvestStatus] = Field(default=None, index=True)
        # harvest_fm_start: Optional[datetime] = None
        # harvest_fm_end: Optional[datetime] = None
        # harvest_fm_status: Optional[HarvestStatus] = Field(default=None, index=True)

    datasets: List["DatasetModel"] = Relationship(back_populates="repository")

class DatasetModel(SQLModel, table=True):
    __tablename__ = "dataset"
    id: Optional[int] = Field(default=None, primary_key=True)
    repo_id: int = Field(foreign_key="repository.id", index=True)
    pid: str = Field(unique=True, index=True)
    pid_protocol: Optional[str]
    timestamp: datetime
    # publisher: Optional[str] = None
    publication_date: Optional[datetime] = Field(default=None, index=True)
    # subject: Optional[str] = None
    # language: Optional[str] = None
    harvest_fm_start: Optional[datetime] = None
    harvest_fm_end: Optional[datetime] = None
    harvest_fm_status: Optional[HarvestStatus] = Field(default=None, index=True)
    files: List["FileMetaDataModel"] = Relationship(back_populates="dataset")

    repository: Optional[RepositoryModel] = Relationship(back_populates="datasets")


class FileMetaDataModel(SQLModel, table=True):
    __tablename__ = "file_metadata"
    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    name: str = Field(index=True)
    link: str = Field(index=True)
    size: int = Field(sa_column=Column(BigInteger))
    mime_type: str = Field(index=True)
    checksum_value: Optional[str] = Field(default=None)
    checksum_type: str = Field(index=True)
    access_request: bool = Field(default=False, index=True)
    publication_date: Optional[datetime] = Field(default=None, index=True)
    embargo: Optional[datetime] = Field(default=None)
    file_pid: Optional[str]
    dataset_pid: str = Field(foreign_key="dataset.pid")

    dataset: Optional["DatasetModel"] = Relationship(back_populates="files")


def create_tables():
    SQLModel.metadata.create_all(engine, checkfirst=True)


def insert_repo(repo: RepositoryModel):
    with Session(engine) as session:
        try:
            session.add(repo)
            session.commit()
            session.refresh(repo)
            return repo.id
        except IntegrityError as e:
            logging.error(f"Integrity error occurred: {e}")
            session.rollback()
            return "Integrity error occurred, repository not inserted."
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            session.rollback()
            return "An error occurred, repository not inserted."


def insert_dataset(dataset: DatasetModel):
    with Session(engine) as session:
        try:
            session.add(dataset)
            session.commit()
            session.refresh(dataset)
            return dataset
        except IntegrityError as e:
            logging.error(f"Integrity error occurred: {e}")
            session.rollback()
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            session.rollback()

def insert_file_metadata(file_metadata: FileMetaDataModel):
    try:
        with Session(engine) as session:
            session.add(file_metadata)
            session.commit()
            session.refresh(file_metadata)
            return file_metadata
    except IntegrityError as e:
        logging.error(f"Integrity error occurred: {e}")
        print(f"Integrity error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

def get_repo_by_id(repo_id: int) -> Optional[RepositoryModel]:
    with Session(engine) as session:
        return session.query(RepositoryModel).options(selectinload(RepositoryModel.datasets)).get(repo_id)

def get_repo_by_prefix_and_url(metadata_prefix: str, url: str) -> Optional[RepositoryModel]:
    with Session(engine) as session:
        return session.query(RepositoryModel).filter(
            RepositoryModel.metadata_prefix == metadata_prefix,
            RepositoryModel.url == url
        ).first()

def get_all_repos() -> List[RepositoryModel]:
    with Session(engine) as session:
        return session.query(RepositoryModel).order_by(RepositoryModel.id).all()


def update_repository_harvest_info(
    repo_id: int,
    harvest_start: Optional[datetime] = None,
    harvest_end: Optional[datetime] = None,
    harvest_status: Optional[str] = None,
) -> Optional[RepositoryModel]:
    with Session(engine) as session:
        repo = session.get(RepositoryModel, repo_id)
        if not repo:
            return None
        if harvest_start is not None:
            repo.harvest_ds_start = harvest_start
        if harvest_end is not None:
            repo.harvest_ds_end = harvest_end
        if harvest_status is not None:
            repo.harvest_ds_status = harvest_status
        session.add(repo)
        session.commit()
        session.refresh(repo)
        return repo

def get_file_metadata_count() -> int:
    with Session(engine) as session:
        return session.query(FileMetaDataModel).count()

def get_dataset_count() -> int:
    with Session(engine) as session:
        return session.query(DatasetModel).count()

def get_dataset_count_by_repo_id(repo_id: int) -> int:
    with Session(engine) as session:
        return session.query(DatasetModel).filter(DatasetModel.repo_id == repo_id).count()

def get_file_metadata_count_by_repo_id(repo_id: int) -> int:
    with Session(engine) as session:
        return (
            session.query(FileMetaDataModel)
            .join(DatasetModel, FileMetaDataModel.dataset_pid == DatasetModel.pid)
            .filter(DatasetModel.repo_id == repo_id)
            .count()
        )

def get_dataset_count_by_repo_id_and_status(repo_id: int, harvest_status: HarvestStatus) -> int:
    with Session(engine) as session:
        return (
            session.query(DatasetModel)
            .join(RepositoryModel, DatasetModel.repo_id == RepositoryModel.id)
            .filter(
                DatasetModel.repo_id == repo_id,
                RepositoryModel.harvest_ds_status == harvest_status
            )
            .count()
        )

def get_dataset_count_by_repo_id_and_fm_status(repo_id: int, harvest_status: HarvestStatus) -> int:
    with Session(engine) as session:
        return (
            session.query(DatasetModel)
            .filter(
                DatasetModel.repo_id == repo_id,
                DatasetModel.harvest_fm_status == harvest_status
            )
            .count()
        )

from sqlalchemy import func

def get_file_metadata_count_grouped_by_mime_type():
    with Session(engine) as session:
        results = (
            session.query(FileMetaDataModel.mime_type, func.count(FileMetaDataModel.id))
            .group_by(FileMetaDataModel.mime_type)
            .order_by(func.count(FileMetaDataModel.id).desc())
            .all()
        )
        return [{"mime_type": mime_type, "count": count} for mime_type, count in results]
from sqlalchemy import func

def get_file_metadata_count_grouped_by_mime_type_by_repo_id(repo_id: int):
    with Session(engine) as session:
        results = (
            session.query(FileMetaDataModel.mime_type, func.count(FileMetaDataModel.id))
            .join(DatasetModel, FileMetaDataModel.dataset_pid == DatasetModel.pid)
            .filter(DatasetModel.repo_id == repo_id)
            .group_by(FileMetaDataModel.mime_type)
            .all()
        )
        return [{"mime_type": mime_type, "count": count} for mime_type, count in results]

from sqlalchemy import func

def get_total_file_size_by_repo_id(repo_id: int) -> int:
    with Session(engine) as session:
        total_size = (
            session.query(func.coalesce(func.sum(FileMetaDataModel.size), 0))
            .join(DatasetModel, FileMetaDataModel.dataset_pid == DatasetModel.pid)
            .filter(DatasetModel.repo_id == repo_id)
            .scalar()
        )
        return total_size

def update_dataset_harvest_fm_start_in_progress(pid: str) -> Optional["DatasetModel"]:
    with Session(engine) as session:
        dataset = session.query(DatasetModel).filter(DatasetModel.pid == pid).first()
        if not dataset:
            return None
        dataset.harvest_fm_start = datetime.now()
        dataset.harvest_fm_status = HarvestStatus.IN_PROGRESS
        session.add(dataset)
        session.commit()
        session.refresh(dataset)
        return dataset

def update_dataset_harvest_fm_end_completed(pid: str) -> Optional["DatasetModel"]:
    with Session(engine) as session:
        dataset = session.query(DatasetModel).filter(DatasetModel.pid == pid).first()
        if not dataset:
            return None
        dataset.harvest_fm_end = datetime.now()
        dataset.harvest_fm_status = HarvestStatus.COMPLETED
        session.add(dataset)
        session.commit()
        session.refresh(dataset)
        return dataset

def delete_file_metadata_by_dataset_pid(dataset_pid: int) -> int:
    with Session(engine) as session:
        deleted = session.query(FileMetaDataModel).filter(FileMetaDataModel.dataset_pid == dataset_pid).delete()
        session.commit()
        return deleted

def dataset_exists(pid: str, repo_id: int) -> bool:
    with Session(engine) as session:
        return (
            session.query(DatasetModel)
            .filter(DatasetModel.pid == pid, DatasetModel.repo_id == repo_id)
            .first()
            is not None
        )

from sqlalchemy import func, extract

def get_dataset_count_grouped_by_publication_month(repo_id: int):
    with Session(engine) as session:
        results = (
            session.query(
                extract('year', DatasetModel.publication_date).label('year'),
                extract('month', DatasetModel.publication_date).label('month'),
                func.count(DatasetModel.id).label('count')
            )
            .filter(DatasetModel.repo_id == repo_id)
            .group_by('year', 'month')
            .order_by('year', 'month')
            .all()
        )
        return [
            {"year": int(year), "month": int(month), "count": count}
            for year, month, count in results
            if year is not None and month is not None
        ]

def get_dataset_count_grouped_by_repo():
    with Session(engine) as session:
        results = (
            session.query(RepositoryModel.name, func.count(DatasetModel.id).label("count"))
            .join(DatasetModel, DatasetModel.repo_id == RepositoryModel.id)
            .group_by(RepositoryModel.name)
            .all()
        )
        return [{"repo-name": name, "dataset-count": count} for name, count in results]

def get_file_metadata_count_grouped_by_repo():
    with Session(engine) as session:
        results = (
            session.query(RepositoryModel.name, func.count(FileMetaDataModel.id).label("count"))
            .join(DatasetModel, DatasetModel.repo_id == RepositoryModel.id)
            .join(FileMetaDataModel, FileMetaDataModel.dataset_pid == DatasetModel.pid)
            .group_by(RepositoryModel.name)
            .all()
        )
        return [{"repo-name": name, "file-metadata-count": count} for name, count in results]

def format_size(size_bytes: int) -> str:
    units = [("T", 1024 ** 4), ("Gb", 1024 ** 3), ("Mb", 1024 ** 2), ("Kb", 1024), ("byte", 1)]
    parts = []
    for name, count in units:
        value = size_bytes // count
        if value:
            parts.append(f"{value} {name}")
            size_bytes %= count
    return " and ".join(parts) if parts else "0 byte"

def get_total_file_size_grouped_by_repo():
    with Session(engine) as session:
        results = (
            session.query(
                RepositoryModel.name,
                func.coalesce(func.sum(FileMetaDataModel.size), 0).label("total_size")
            )
            .join(DatasetModel, DatasetModel.repo_id == RepositoryModel.id)
            .join(FileMetaDataModel, FileMetaDataModel.dataset_pid == DatasetModel.pid)
            .group_by(RepositoryModel.name)
            .all()
        )
        return [
            {
                "repository-name": name,
                "total-file-size": int(total_size),
                "total-file-size-in-friendly": format_size(int(total_size))
            }
            for name, total_size in results
        ]

def get_repo_by_dataset_pid(dataset_pid: str) -> Optional[RepositoryModel]:
    with Session(engine) as session:
        dataset = session.query(DatasetModel).filter(DatasetModel.pid == dataset_pid).first()
        if not dataset:
            return None
        repo = session.query(RepositoryModel).get(dataset.repo_id)
        return repo

def get_repo_by_file_metadata_link(file_link: str) -> Optional[RepositoryModel]:
    with Session(engine) as session:
        file_metadata = session.query(FileMetaDataModel).filter(FileMetaDataModel.link == file_link).first()
        if not file_metadata:
            return None
        dataset = session.query(DatasetModel).filter(DatasetModel.pid == file_metadata.dataset_pid).first()
        if not dataset:
            return None
        repo = session.query(RepositoryModel).get(dataset.repo_id)
        return repo