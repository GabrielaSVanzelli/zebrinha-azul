import os
import sys
from typing import List, Optional, Literal
import requests
from requests import RequestException, Response
import logging
import json
import io
from datetime import datetime

logger = logging.getLogger(__name__)

class MakeRequests():
    def __init__(self,
                 req_type: Literal["post", "get"],
                 url: str,
                 params: Optional[dict] = None) -> None:
        self.req_type = req_type
        self.url = url
        self.params = params

    def req(self) -> Optional[Response]:
        for retry in range(10):
            try:
                method = getattr(requests, self.req_type)
                response = method(url=self.url, params=self.params)
                logger.info(response.status_code)
                if response.status_code != 200:
                    raise requests.exceptions.ConnectionError
                logger.info(
                    f"{self.req_type.upper()} {retry} - Status Code 200 | API servers are working! "
                    f"Last Updated: {datetime.now()}"
                )
                return response
            except RequestException as e:
                logger.error(f"ERROR | {e}")

class APIClient:
    """
    Class for interacting with an API.
    """

    def __init__(self,
                 url_weather_api: Optional[str]=None,
                 url_traffic_api: Optional[str]=None,
                 key_weather_api: Optional[str]=None,
                 key_traffic_api: Optional[str]=None) -> None:
        self.url_weather = url_weather_api
        self.url_traffic = url_traffic_api
        self.key_weather_api = key_weather_api
        self.key_traffic_api = key_traffic_api

    def fetch_data_weather(self,
                           city: str) -> Optional[requests.Response]:
        """
        Fetches data from the Weather API

        Returns:
            dict: Response JSON data.
        """
        try:
            params = {
                "q": city,
                "appid": self.key_weather_api,
                "units": "metric"
            }
            logger.info("Fetching data")
            response = MakeRequests(req_type="get", url=self.url_weather, params=params).req()
            return response
        except RequestException as e:
            logger.info(e)
            return None
    
    def fetch_data_traffic(self,
                           origin: str,
                           destination: str) -> Optional[requests.Response]:
        """
        Fetches data from the Traffic API

        Returns:
            dict: Response JSON data.
        """
        try:
            logger.info("Fetching data")
            params = {
                'origin': origin,
                'destination': destination,
                'key': self.key_traffic_api
            }

            response = MakeRequests(req_type="get", url=self.url_traffic, params=params).req()
            return response
        except RequestException as e:
            logger.info(e)
            return None
        
class DataParser:
    """
    Class for parsing data.
    """

    @staticmethod
    def parse_traffic_data(data: requests.Response) -> Optional[dict]:
        """
        Parses JSON data into instances of the Trajectory model

        Args:
            data (dict): JSON data to parse.

        Returns:
            List[Trajectory]: Json parsed.
        """
        try:
            logger.info("Parsing data")
            data = data.json()
            parsed_data = json.dumps(data, indent=4)
            logger.info("Data parsing successful!")
            return parsed_data
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None

    @staticmethod
    def parse_weather_data(data: requests.Response):
        """
        Parses JSON data into instances of the Weather model

        Args:
            data (dict): JSON data to parse.

        Returns:
            List[WeatherResponse]: Json parsed.
        """
        try:
            logger.info("Parsing data")
            data = data.json()
            parsed_data = json.dumps(data, indent=4).encode(
                "utf-8"
            )
            logger.info("Data parsing successful!")
            return parsed_data
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None