import aiohttp, asyncio
from models import PipelineConfigs
import logging, requests
from typing import Optional

class Resource: 
    def __init__(self, name: str, url: str, package_name: str):
        self.name = name
        self.url = url
        self.package_name = package_name

    async def download(self, session: aiohttp.ClientSession):
        pass

class Package:
    def __init__(
            self, 
            configs: PipelineConfigs, 
            package_name: str
        ):
        self.name = package_name
        self.resources = self.get_resources()
        self.logger = configs.logger
    
    async def get(self):
        async with aiohttp.ClientSession() as session:
            tasks = [resource.download(session=session) for resource in self.resources]
            return await asyncio.gather(*tasks)

    def get_resources(self):
        response = self._request_resource_list()
        if response:
            return self._process_resource_library(response=response)

    def _request_resource_list(self) -> requests.Response:
        """
        Requests the resource library for a particular package from Open Data BCN.

        Args: 
            logger (logging.Logger): A logging instance for recording events.
            package_name (str): Name of an Open Data BCN package containing multiple resources.

        Returns:
            The requests.Response object with all the info from the response
        """

        url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'
        
        try:
            response = requests.get(url, params={'id': self.name}, timeout=10)
            return response
        
        except requests.exceptions.ConnectTimeout:
            self.logger.error(f"Connection to Open Data BCN timed out while requesting package details for '{self.name}'.")
        except requests.exceptions.Timeout:
            self.logger.error(f"Request for package details for '{self.package_name}' timed out.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch package '{self.name}': {e.__class__.__name__} - {e}")
            self.logger.debug("Full exception details:", exc_info=True)
    
    def _process_resource_library(
        self,
        response: requests.Response,
        ) -> list[Resource]:
    
        """
        Processes the request object to extract a package resource library into a list of dictionaries.

        Args:
            logger (logging.Logger): A logging instance for recording events.
            response (requests.Response): A raw response object containing package information.
            package (str): The name of the package being processed.
        Returns:
            A list of dictionaries.
        """
        data = response.json()

        try:
            resources = data['result']['resources']
        except Exception as e:
            self.logger.exception(f"There was a problem accessing the resources from the request object: {e}")
            return None
        csv_resources = []
        for res in resources:
            if res['format'] == "CSV":
                r = Resource(
                    name=res["name"], 
                    url=res["url"], 
                    package_name=self.name)
                csv_resources.append(r)
        return csv_resources
    


class Report:

    def __init__(self, package: str, start_time: time.time):
        self.package_name = package
        self.package_success = False
        self.package_response_code = None
        self.num_resources = 0
        self.resources_success = []
        self.resources_fail = []
        self.total_duration = 0
        self.start_time = 0
        self.end_time = 0
        self.num_errors = 0
        self.skipped = 0

    def process_package_response(self, response):
        self.total_duration += response.elapsed.total_seconds()
        self.package_response_code = response.status_code
    
    def process_resource_response(self, response, resource):
        self.total_duration += response.elapsed.total_seconds()
        if not response.status_code == 200:
            self.resources_fail.append(resource)
        else:
            self.resources_success.append(resource['name'])
    
    def add_resources_fail(self, resource):
        self.resources_fail.append(resource)

    def add_to_total_duration(self, seconds):
        self.total_duration += seconds