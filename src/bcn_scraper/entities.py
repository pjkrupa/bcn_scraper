import aiohttp, asyncio, time, random, os
from models import PipelineConfigs, ResourceReport
import logging, requests
from utilities import save_csv, convert_to_csv


class Resource: 
    def __init__(
            self, 
            name: str, 
            url: str, 
            package_name: str, 
            logger: logging.Logger,
            save_path: str,
            configs: PipelineConfigs
            ):
        self.configs = configs
        self.name = name
        self.url = url
        self.package_name = package_name
        self.logger = logger
        self.save_path = save_path
        self.report = ResourceReport(name=self.name)

    # async doesn't give any advantage with the BCN Open Data portal
    # because requests are limited to 1/sec, but 
    async def download(
            self, 
            session: aiohttp.ClientSession,
        ) -> ResourceReport:
        
        self.report.start = time.time()
        dir_path = os.path.join(
            self.save_path, 
            self.package_name,
            )
        os.makedirs(dir_path, exist_ok=True)
        filepath = os.path.join(dir_path, self.name)

        self.logger.info(f"Downloading resource {self.name}...")
        try:
            # randomly stutters the start of the downloads
            await asyncio.sleep(random.uniform(0.1, 0.5))
            response = await self._request_with_retry(session=session)
            with open(filepath, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
            await response.release()
        except Exception as e:
            self.logger.error(f"Failed to download {self.name}: {e}")
            self.report.error = True

        finally:
            self.report.end = time.time()
            download_time = self.report.end - self.report.start

            # The logs are yellow if the process took more than 5 seconds. Move this into the logging format eventually
            if download_time > 5:
                self.logger.warning(f"\033[33mResource {self.name} downloaded in {round(download_time, 3)} seconds.\033[0m")
            else:
                self.logger.info(f"Resource {self.name} downloaded in {round(download_time, 3)} seconds.")
            return self.report

    async def _request_with_retry(self, session: aiohttp.ClientSession):
        retries = 5

        for attempt in range(retries):
            try:
                await self.configs.wait_for_slot()
                response = await session.get(url=self.url)
                
                if response.status == 200:
                    return response
                
                status = response.status if response.status else "No response recieved"
            except Exception as e:
                self.logger.warning(f"The request failed: {status} -- {e}")
                
            if attempt < retries - 1:
                wait = 2 ** attempt
                self.logger.info(f"Retrying in {wait}s...")
                await asyncio.sleep(wait)
            
        raise Exception(f"Failed to download {self.name} after {retries} attempts")


    async def _persistant_request(
            self, 
            session: aiohttp.ClientSession
            ) -> aiohttp.ClientResponse:
        
        max_retries = 5
        attempts_remaining = max_retries

        backoff_factor = 2
        for _ in range(0,max_retries):
            self.report.tries+=1
            await asyncio.sleep(random.uniform(0.1, 0.5)) # this adds a little jitter so all the requests don't hit the server at once.
            response = await session.get(url=self.url, timeout=10)

            if response and response.status == 200:
                self.success = True
                return response
            else:
                if response:
                    status = response.status
                else:
                    status = "No response."
                self.logger.error(f"Problem with response from server: {status}.")
                self.report.error = True
                attempts_remaining-=1

                if attempts_remaining > 0:
                    wait_time = backoff_factor * (2 ** (max_retries - attempts_remaining))
                    self.logger.info(f"Trying again in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.warning(f"Out of attempts.")
                    return response
    

class Package:
    def __init__(
            self, 
            configs: PipelineConfigs, 
            package_name: str
        ):
        self.configs = configs
        self.logger = configs.logger
        self.name = package_name
        self.resources = self.get_resources()
        self.report = Report(
            package=package_name,
            start_time=time.time()
        )
    
    async def get(self):
        async with aiohttp.ClientSession() as session:
            downloads = [resource.download(session=session) for resource in self.resources]
            return await asyncio.gather(*downloads)

    # add more logic here to handle problems; right now, it returns None if there's no response.
    def get_resources(self) -> list[Resource]:
        response = self._request_resource_list()
        if response:
            return self._process_resource_library(response=response)

    def _request_resource_list(self) -> requests.Response:

        url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'
        
        try:
            response = requests.get(url, params={'id': self.name}, timeout=10)
            return response
        
        except requests.exceptions.ConnectTimeout:
            self.logger.error(f"Connection to Open Data BCN timed out while requesting package details for '{self.name}'.")
        except requests.exceptions.Timeout:
            self.logger.error(f"Request for package details for '{self.name}' timed out.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch package '{self.name}': {e.__class__.__name__} - {e}")
            self.logger.debug("Full exception details:", exc_info=True)
    
    def _process_resource_library(
        self,
        response: requests.Response,
        ) -> list[Resource]:
    
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
                    package_name=self.name,
                    logger=self.logger,
                    save_path=self.configs.storage_root,
                    configs=self.configs
                    )
                csv_resources.append(r)
        if not csv_resources:
            return None
        else:
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
        self.package_response_code = response.status
    
    def process_resource_response(self, response, resource):
        self.total_duration += response.elapsed.total_seconds()
        if not response.status == 200:
            self.resources_fail.append(resource)
        else:
            self.resources_success.append(resource['name'])
    
    def add_resources_fail(self, resource):
        self.resources_fail.append(resource)

    def add_to_total_duration(self, seconds):
        self.total_duration += seconds