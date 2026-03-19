import time, asyncio
from dataclasses import dataclass, field
from logging import Logger

@dataclass
class Database:
    db_user: str
    db_host: str
    db_name: str

@dataclass
class PipelineConfigs:
    db: Database
    base_url: str
    logger: Logger
    packages: list[str]
    storage_root: str

    # rate limiter that sets the request interval and request concurrency
    rate_interval: float
    request_concurrency: int
    rate_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_request_time: float = 0.0

    async def wait_for_slot(self):
        async with self.rate_lock:
            now = time.monotonic()
            wait = self.rate_interval - (now - self.last_request_time)
            if wait > 0:
                await asyncio.sleep(wait)

            self.last_request_time = time.monotonic()

@dataclass
class ResourceReport:
    name: str
    success: bool = False
    error: bool = False
    tries: int = 0
    start: float = 0
    end: float = 0