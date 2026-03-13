import logging, time, argparse, requests
from argparse import ArgumentParser
import pandas as pd
from entities import Report
from typing import Optional


def compile_reports(report_list: list[Report]):
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

def persistant_request(
        logger: logging.Logger,
        report: Report,
        resource: dict = None,
        package: str = None,
        backoff_factor: int = 2,
        max_retries: int = 3,
        ) -> Optional[requests.Response]:
    
    """
    A function for making multiple tries at retrieving package info and resources.
    Works on requests for both packages and resources

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource. If getting a package, leave None.
        package (str): Name of an Open Data BCN package containing multiple resources. If getting a resource, leave None.
        backoff_factor (int): Keeps the script from hammering the servers too much.
        max_retries (int): Maximum number of times the loop retries the request

    Returns:
        A request.Response object if a response is received, or None if no response is received.
    """

    attempts_remaining = max_retries

    while attempts_remaining > 0:
        # This is to check if the function call is for a resource or a package.
        if resource:
            response = download_resource(logger, resource)
        else:
            response = request_resource_library(logger, package)
            
            

        if response and response.status_code == 200:
            return response
        else:
            if response:
                status = response.status_code
            else:
                status = "No response."
            logger.error(f"Problem with response from server: {status}.")
            report.num_errors += 1
            attempts_remaining -= 1

            if attempts_remaining > 0:
                wait_time = backoff_factor * (2 ** (max_retries - attempts_remaining))
                logger.info(f"Trying again in {wait_time} seconds...")
                time.sleep(wait_time)

        logger.warning(f"Out of attempts.")
        return response