import os, datetime
from logging import Logger
from dotenv import load_dotenv
from utilities import get_logger, get_parser, get_args
from models import Database, PipelineConfigs
from pipeline import pipeline
from entities import Package


def run_pipeline(configs: PipelineConfigs):
    configs.logger.info(f"Starting the pipeline at {datetime.datetime.now()}")
    for package in configs.packages:
        p = Package(configs=configs, package_name=package)
        try:
            p.get()
        except Exception as e:
            configs.logger.error(f"something went wrong: {e}")

if __name__ == "__main__":
    
    # initial setup:
    load_dotenv()
    parser=get_parser()
    packages, storage_root = get_args(parser)

    configs = PipelineConfigs(
        db=Database(
            db_name = os.getenv("DB_NAME"),
            db_host = os.getenv("DB_HOST"),
            db_user = os.getenv("DB_USER")
        ),
        logger=get_logger(),
        packages=packages,
        storage_root=storage_root
    )
    
    run_pipeline(configs=configs)