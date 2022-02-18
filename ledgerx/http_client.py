import requests
import aiohttp
import asyncio
from typing import Dict
from time import sleep
from ledgerx.util import gen_headers
from ledgerx import DELAY_SECONDS

import logging

logger = logging.getLogger(__name__)

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
            logger.info(f"getting {url} {'Authorization' in headers} {params}")
            res = requests.get(url, headers=headers, params=params)
            logger.debug(f"get {url} {res}")
            if res.status_code == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                sleep(delay)
            else:
                if res.status_code != 200:
                    logger.warning(f"res={res} url={url} params={params}")
                res.raise_for_status()
                break
        return res

    @staticmethod
    def post(
        url: str, data: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
    ) -> requests.Response:
        """Execute http post request

        Args:
            url (str): [description]
            data (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            requests.Response: [description]
        """
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        res = None
        while True:
            res = requests.post(url, headers=headers, json=data)
            if res.status_code == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                sleep(delay)
            else:
                if res.status_code != 200:
                    logger.warning(f"res={res} url={url} json={data}")
                logger.debug(f"post {url} {res}")
                res.raise_for_status()
                break
        return res

    @staticmethod
    def delete(
        url: str, params: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
    ) -> requests.Response:
        """Execute http delete request

        Args:
            url (str): [description]
            params (Dict, optional): [description]. Defaults to {}.
            include_api_key (bool, optional): [description]. Defaults to False.

        Returns:
            [type]: [description]
        """
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        res = None
        while True:
            logger.info(f"Executing delete url={url} params={params}")
            res = requests.delete(url, headers=headers, params=params)
            if res.status_code == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                sleep(delay)
            else:
                if res.status_code != 200:
                    logger.warning(f"res={res} url={url} params={params}")
                logger.debug(f"delete {url} {res}")
                res.raise_for_status()
                break
        return res

    aiohttp_session = None
    @classmethod
    def bootstrap_aiohttp_session(cls):
        loop = asyncio.get_event_loop()
        if cls.aiohttp_session is None:
            logger.info(f"new aiohttp.ClientSession")
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
        clean_params = {}
        for k,v in params.items():
            if v is not None:
                clean_params[k] = v
        
        while True:
            logger.info(f"getting {url} {'Authorization' in headers} {clean_params}")
            #res = await loop.run_in_executor(None, requests.get, url, dict(**params, headers=headers))
            res = await aiohttp_session.get(url, headers=headers, params=clean_params)
            logger.debug(f"got from {url} : res={res}")

            if res.status == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                await asyncio.sleep(delay)
            else:
                if res.status != 200:
                    logger.warning(f"res={res} url={url} params={params}")
                res.raise_for_status()
                return res

    @classmethod
    async def async_post(cls,
        url: str, data: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
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
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        while True:
            res = await aiohttp_session.post(url, headers=headers, json=data)
            logger.debug(f"post {url} {res}")
            if res.status == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                await asyncio.sleep(delay)
            else:
                if res.status != 200:
                    logger.warning(f"res={res} url={url} data={data}")
                res.raise_for_status()
                return res

    @classmethod
    async def async_delete(cls,
        url: str, params: Dict = {}, include_api_key: bool = False, NO_RETRY_429_ERRORS: bool = False
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
        delay = DELAY_SECONDS
        headers = gen_headers(include_api_key)
        while True:
            res = await aiohttp_session.delete(url, params=params, headers=headers)
            logger.debug(f"delete {url} {res}")
            if res.status == 429 and HttpClient.RETRY_429_ERRORS and not NO_RETRY_429_ERRORS:
                if delay == DELAY_SECONDS:
                    delay += 1
                else:
                    delay *= 2.0
                if delay > 10:
                    delay += DELAY_SECONDS + 1
                logger.info(f"Got 429, delaying {delay}s before retry of url: {url}")
                await asyncio.sleep(delay)
            else:
                if res.status != 200:
                    logger.warning(f"res={res} url={url} params={params}")
                res.raise_for_status()
                return res



