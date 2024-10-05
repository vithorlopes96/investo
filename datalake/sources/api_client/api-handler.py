import os
import structlog
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import setup_logger
from utils.event_handler import EnvTokenFetcher

# ApiCaller class (Handles API calls, SRP and OCP)
class ApiCaller:
    def __init__(self, token_fetcher: TokenFetcher, logger):
        self.token_fetcher = token_fetcher
        self.logger = logger

    def make_api_call(self, url, task):
        """Handles a single API call with the provided task parameters."""
        token = self.token_fetcher.fetch_token()
        headers = {
            'Authorization': f"Bearer {token}",
            'Content-Type': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers, params=task)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error("Failed API call", status_code=response.status_code, task=task)
                return None
        except requests.RequestException as e:
            self.logger.error("Request failed", task=task, error=str(e))
            return None


# Request class (Manages task execution with API calls, SRP)
class Request:
    def __init__(self, api_caller: ApiCaller, logger, tasks):
        self.api_caller = api_caller
        self.logger = logger
        self.tasks = tasks

    def execute_tasks(self):
        """Executes API tasks in parallel using ThreadPoolExecutor."""
        if not self.tasks:
            self.logger.error("No tasks provided for API calls.")
            return {}

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = {executor.submit(self.api_caller.make_api_call, task): task for task in self.tasks}
            results = {}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    self.logger.info("Task completed", task=task, result=result)
                    results[task] = result
                except Exception as exc:
                    self.logger.error("Task generated an exception", task=task, error=str(exc))
                    results[task] = None
            return results

