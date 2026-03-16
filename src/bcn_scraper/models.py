import time
from dataclasses import dataclass
from logging import Logger

@dataclass
class Database:
    db_user: str
    db_host: str
    db_name: str

@dataclass
class PipelineConfigs:
    db: Database
    logger: Logger
    packages: list[str]
    storage_root: str

@dataclass
class ResourceReport:
    name: str
    success: bool = False
    error: bool = False
    tries: int = 0
    start: float = 0
    end: float = 0