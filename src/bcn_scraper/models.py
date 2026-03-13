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
