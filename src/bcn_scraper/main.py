import os, datetime, asyncio
from logging import Logger
from dotenv import load_dotenv
from utilities import get_logger, get_parser, get_args
from models import Database, PipelineConfigs
from entities import Package, Report


def run_pipeline(configs: PipelineConfigs):
    configs.logger.info(f"Starting the pipeline at {datetime.datetime.now()}")

    final = []
    for package in configs.packages:
        try:
            p = Package(configs=configs, package_name=package)
            if p.resources is None:
                configs.logger.warning(f"No CSV resources found for package {p.name}, skipping...")
                continue
            else:
                configs.logger.info(f"Successfully instantiated package {p.name}. Downloading resources...")
        except Exception as e:
            configs.logger.error(f"Couldn't instantiate package {package}: {e}")
            continue

        try:
            asyncio.run(p.get())
            final.append(p)
        except Exception as e:
            configs.logger.error(f"Error running package {p.name}: {e}")
    
    

if __name__ == "__main__":
    
    # initial setup:
    load_dotenv()
    parser=get_parser()
    packages, storage_root = get_args(parser)
    ts = datetime.datetime.now().replace(microsecond=0)
    logs_path = f"{ts.strftime('%Y%m%d_%H%M%S')}.log"

    configs = PipelineConfigs(
        db=Database(
            db_name = os.getenv("DB_NAME"),
            db_host = os.getenv("DB_HOST"),
            db_user = os.getenv("DB_USER")
        ),
        logger=get_logger(path=logs_path),
        packages=packages,
        storage_root=storage_root,
        base_url = os.getenv("BASE_URL")
    )
    
    run_pipeline(configs=configs)