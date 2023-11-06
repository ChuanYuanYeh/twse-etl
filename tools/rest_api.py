"""
Class for sending HTTP requests
"""
import requests

from config.config import config
from tools.common import keep_retries


class RestAPI:
    """
    The RestAPI object is used to interact with a configured API service.
    """
    def __init__(self, url: str) -> None:
        """
        Instantiates a RestAPI object.

        Parameters
        ----------
        url: str - The base URL of the API service

        Returns
        ----------
        None
        """
        self.base_url = url

    @keep_retries
    def send_get_request(self, endpoint: str='', params: dict=None) -> dict:
        """
        Sends a GET request to the base_url with optional endpoint.

        Parameters
        ----------
        endpoint: str - Optional endpoint to append to base_url
        params: dict - Optional parameters to send in request

        Returns
        ----------
        str - HTTP response in plain text
        """
        full_url = f'{self.base_url}/{endpoint}'
        resp = requests.get(full_url, params=params)
        
        if resp.status_code != 200:
            raise Exception(f'Failed to send request to url: {full_url} with status_code: {resp.status_code}')
        
        return resp.text
