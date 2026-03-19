import logging, time, argparse, requests, csv, os
from argparse import ArgumentParser
import pandas as pd
from io import StringIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from entities import Resource, Report
    from models import PipelineConfigs


def compile_reports(report_list: list["Report"]):
    final_report = {
        'packages_success': [],
        'packages_fail': [],
        'resources_success': [],
        'resources_fail': [],
        'total_duration': 0,
        'num_errors': 0,
        'skipped': 0,
    }

    for report in report_list:
        final_report['num_errors'] += report.num_errors
        final_report['total_duration'] = report.end_time - report.start_time
        if not report.package_success:
            final_report['packages_fail'].append(report.package_name)
            continue
        final_report['packages_success'].append(report.package_name)
        final_report['resources_success'].extend(report.resources_success)
        final_report['resources_fail'].extend(report.resources_fail)
        final_report['skipped'] += report.skipped
    return final_report

def get_args(parser: ArgumentParser) -> list[str]:
    """
    Parses the command line arguments and 
    """
    args = parser.parse_args()
    if args.packages:
        package_list = args.packages
    else:
        package_list = get_packages(args.tags)
    return package_list, args.directory

def get_packages(tags: list[str]) -> list[str]:
    """
    Returns a list of all packages with the submitted tags.
    """
    df = pd.read_csv("catalog_tags.csv")
    packages = []
    for tag in tags:
        result = df.loc[
            df['tags_list'].apply(
                lambda x: isinstance(x, str) and tag.lower() in [t.strip().lower() for t in x.split(',')]
                ),
                'name'
        ]
        packages.extend(result.to_list())
    packages = list(set(packages))
    return packages

def get_logger(
        name='__name__', 
        path='etl.log', 
        level=logging.INFO,
        ):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    file_handler = logging.FileHandler(
        filename=path,
        encoding="utf-8",
        mode="a",
    )
    console_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    formatter = logging.Formatter( 
        "{asctime} - {levelname} - {message}", 
        style="{",
        datefmt="%H:%M:%S",
    )

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    return logger

def get_parser():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-p", 
        "--packages",
        nargs='+',
        help="Name of the Open Data BCN package listing the resources available."
        )
    group.add_argument(
        "-t", 
        "--tags",
        nargs='+',
        help="Tags to be searched for in the BCN catelog. All packages with a tag will be downloaded."
        )
    parser.add_argument(
        '-d',
        '--directory',
        default='.',
        help='Root directory where you want to save the CSV files',
    )
    parser.add_argument(
        "--to_db",
        action="store_true",
        help="Set this flag if you want to automatically ingest the CSV files into a database after download"
    )
    return parser

def convert_to_csv(
        logger: logging.Logger,  
        response: requests.Response
        ) -> StringIO:
    """
    Converts a response.Requests object to a CSV StringIO object.
    """
    try:
        try:
            decoded = response.content.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("UTF-8 decoding failed, retrying with UTF-16.")
            decoded = response.content.decode('utf-16')

        csv_data = StringIO(decoded)
        reader = csv.reader(csv_data)

        #simple check to see if the file is actually a CSV. if not, it throws an error.
        first_row = next(reader)
        
        logger.info(f"Successfully converted response object to a CSV file.")
        return csv_data

    except csv.Error as e:
        logger.exception(f"Response content is not valid CSV format: {e}")
        return None
    
    except StopIteration:
        logger.warning("CSV appears to be empty.")
        return None

    except Exception as e:
        logger.exception(f"There was an error while converting the response into a CSV: {e}")
        return None
    
def save_csv(logger: logging.Logger, 
             resource: "Resource", 
             csv: StringIO, 
             save_path: str="./"
             ) -> bool:
    """
    Saves a CSV in-memory object to disk.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary containing information about the resource.
        csv (StringIO): A StringIO object that is a CSV file in memory.
        path (str): Parameter provided by user indicating where to save file (default: root)

    Returns:
        A boolean operator indicating if the operation was successful or not.
    """
    csv.seek(0)
    
    logger.info("Saving CSV to disk...")

    dir_path = os.path.join(
        save_path, 
        resource.package_name,
        )
    
    try:
        os.makedirs(dir_path, exist_ok=True)
    except PermissionError as e:
        logger.error(f"Sorry, you don't have permission to create the directory {dir_path}: {e}")
        return False
    
    final_path = os.path.join(dir_path, resource.name)

    try:
        with open(final_path, 'w', encoding='utf-8') as f:
            f.write(csv.getvalue())
        logger.info(f"Succesfully saved CSV file to {final_path}.")
        return True
    
    except Exception as e:
        logger.error(f"There was a problem saving the file: {e}")
        return False

def log_configs(configs: "PipelineConfigs"):
    logger = configs.logger
    logger.info(f"Configurations for this pipeline run...")
    logger.info(f"Base URL: {configs.base_url}")
    logger.info(f"Packages to download: {configs.packages}")
    logger.info(f"Downloads will be saved to: {configs.storage_root}")
    logger.info(f"Request rate interval: {configs.rate_interval}")
    concurrency = None if configs.request_concurrency == 0 else configs.request_concurrency
    logger.info(f"Request concurrency limit: {concurrency}")

# Going to use this function eventually to load CSVs into Postgres.
def to_df(logger: logging.Logger, resource: dict, csv: StringIO) -> pd.DataFrame:

    """
    Takes an in-memory CSV object and returns a pandas dataframe.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary containing information about the resource.
        csv (StringIO): A StringIO object that is a CSV file in memory.
    """
    
    csv.seek(0)
    
    logger.info("-------------------------------------------")
    logger.info(f"Converting to a dataframe...")
    
    try:
        df = pd.read_csv(csv)
        logger.info("Successfully converted CSV to a dataframe!")
        logger.info("-------------------------------------------")
        return df
    except Exception as e:
        logger.exception(f"Something went wrong: {e}")
        logger.info("-------------------------------------------")
        return
