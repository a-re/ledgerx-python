import requests
import aiohttp
import asyncio
from typing import Dict
from time import sleep
from ledgerx.util import gen_headers
from ledgerx import DELAY_SECONDS

import logging

class HttpClient:
    # TODO(weston) - handle rate limiting, https://docs.ledgerx.com/reference#rate-limits
    RETRY_429_ERRORS = False

    @staticmethod
    def get(
        url: str, params: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
    ) -> requests.Response:
        """Excute http get request

        Args:
            url (str): [description]
            params (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            requests.Response: [description]
        """
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        res = None
        while True:
            logging.info(f"getting {url} {'Authorization' in headers} {params}")
            res = requests.get(url, headers=headers, params=params)
            logging.debug(f"get {url} {res}")
            if res.status_code == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay = 10
                logging.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                sleep(delay)
            else:
                res.raise_for_status()
                break
        return res

    @staticmethod
    def post(
        url: str, data: Dict = {}, include_api_key: bool = False
    ) -> requests.Response:
        """Execute http post request

        Args:
            url (str): [description]
            data (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            requests.Response: [description]
        """
        headers = gen_headers(include_api_key)
        res = requests.post(url, headers=headers, json=data)
        logging.debug(f"post {url} {res}")
        res.raise_for_status()
        return res

    @staticmethod
    def delete(
        url: str, params: Dict = {}, include_api_key: bool = False
    ) -> requests.Response:
        """Execute http delete request

        Args:
            url (str): [description]
            params (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            [type]: [description]
        """
        headers = gen_headers(include_api_key)
        res = requests.delete(url, params=params, headers=headers)
        logging.debug(f"delete {url} {res}")
        res.raise_for_status()
        return res

    aiohttp_session = None
    @classmethod
    def bootstrap_aiohttp_session(cls):
        loop = asyncio.get_event_loop()
        if cls.aiohttp_session is None:
            logging.info(f"new aiohttp.ClientSession")
            cls.aiohttp_session = aiohttp.ClientSession(loop=loop)
        return cls.aiohttp_session

    @classmethod
    async def async_get(cls,
        url: str, params: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
    ) -> requests.Response:
        """Execute http get request

        Args:
            url (str): [description]
            params (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            requests.Response: [description]
        """

        aiohttp_session = cls.bootstrap_aiohttp_session()
        
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        res = None
        
        while True:
            logging.info(f"getting {url} {'Authorization' in headers} {params}")
            #res = await loop.run_in_executor(None, requests.get, url, dict(**params, headers=headers))
            res = await aiohttp_session.get(url, headers=headers, params=params)
            logging.debug(f"got from {url} : res={res}")

            if res.status == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay = 10
                logging.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                await asyncio.sleep(delay)
            else:
                res.raise_for_status()
                break
        return res

    @classmethod
    async def async_post(cls,
        url: str, data: Dict = {}, include_api_key: bool = False
    ) -> requests.Response:
        """Execute http post request

        Args:
            url (str): [description]
            data (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            requests.Response: [description]
        """
        aiohttp_session = cls.bootstrap_aiohttp_session()
        headers = gen_headers(include_api_key)
        res = await aiohttp_session.post(url, headers=headers, json=data)
        logging.debug(f"post {url} {res}")
        res.raise_for_status()
        return res

    @classmethod
    async def async_delete(cls,
        url: str, params: Dict = {}, include_api_key: bool = False
    ) -> requests.Response:
        """Execute http delete request

        Args:
            url (str): [description]
            params (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            [type]: [description]
        """
        aiohttp_session = cls.bootstrap_aiohttp_session()
        headers = gen_headers(include_api_key)
        res = await aiohttp_session.delete(url, params=params, headers=headers)
        logging.debug(f"delete {url} {res}")
        res.raise_for_status()
        return res
