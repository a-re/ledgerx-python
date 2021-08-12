from time import sleep
from typing import List, Dict, Callable

from ledgerx import DELAY_SECONDS
from ledgerx.http_client import HttpClient
from ledgerx.util import has_next_url

import logging
import asyncio

logger = logging.getLogger(__name__)

class GenericResource:
    list_all_default_delay = DELAY_SECONDS

    @classmethod
    def next(cls, next_url: str, params: Dict, include_api_key: bool = False):
        res = HttpClient.get(next_url, params, include_api_key)
        json_data = res.json()
        logger.debug(f"next {next_url} got {res} {json_data}")
        return json_data

    @classmethod
    def list(cls, url: str, params: Dict, include_api_key: bool = False):
        res = HttpClient.get(url, params, include_api_key)
        json_data = res.json()
        logger.debug(f"list {url} got {res} {json_data}")
        return json_data

    @classmethod
    def list_all(
        cls,
        url: str,
        params: Dict = {},
        include_api_key: bool = False,
        max_fetches: int = 0,
        delay: float = -1,
    ) -> List[Dict]:
        elements = []
        if delay < 0:
            delay = cls.list_all_default_delay
        json_data = cls.list(url, params, include_api_key)
        elements.extend(json_data["data"])
        fetches = 1

        while has_next_url(json_data):
            if max_fetches > 0 and fetches >= max_fetches:
                break
            sleep(delay)
            json_data = cls.next(json_data["meta"]["next"], params, include_api_key)
            elements.extend(json_data["data"])
            fetches += 1
        return elements

    @classmethod
    def list_all_incremental_return(
        cls,
        url: str,
        params: Dict = {},
        include_api_key: bool = False,
        callback: Callable = None,
        max_fetches: int = 0,
        delay: float = DELAY_SECONDS,
    ) -> None:
        json_data = cls.list(url, params, include_api_key=include_api_key)
        callback(json_data["data"])
        fetches = 1

        while has_next_url(json_data):
            if max_fetches > 0 and fetches >= max_fetches:
                break
            sleep(delay)
            json_data = cls.next(json_data["meta"]["next"], params, include_api_key)
            callback(json_data["data"])
            fetches += 1

    @classmethod
    async def async_next(cls, next_url: str, params: Dict, include_api_key: bool = False):
        logger.info(f"next_url: {next_url} {params}")
        res = await HttpClient.async_get(next_url, params, include_api_key)
        json_data = await res.json()
        logger.debug(f"next {next_url} got {res} {json_data}")
        return json_data

    @classmethod
    async def async_list(cls, url: str, params: Dict, include_api_key: bool = False):
        logger.info(f"calling async_get on {url} {params}")
        res = await HttpClient.async_get(url, params, include_api_key)
        json_data = await res.json()
        logger.debug(f"list {url} got {res} {json_data}")
        return json_data

    @classmethod
    async def async_list_all(
        cls,
        url: str,
        params: Dict = {},
        include_api_key: bool = False,
        max_fetches: int = 0,
        delay: float = -1,
    ) -> List[Dict]:
        elements = []
        if delay < 0:
            delay = cls.list_all_default_delay
        logger.info(f"calling async_list {url} {params}")
        json_data = await cls.async_list(url, params, include_api_key=include_api_key)
        logger.debug(f"Got for {url} : json_data={json_data}")
        elements.extend(json_data["data"])
        fetches = 1

        while has_next_url(json_data):
            if max_fetches > 0 and fetches >= max_fetches:
                break
            await asyncio.sleep(delay)
            logger.info(f"calling async_next {url} {params}")
            json_data = await cls.async_next(json_data["meta"]["next"], params, include_api_key=include_api_key)
            logger.debug(f"Got for {url} : json_data={json_data}")
            elements.extend(json_data["data"])
            fetches += 1
        return elements

    @classmethod
    async def async_list_all_incremental_return(
        cls,
        url: str,
        params: Dict = {},
        include_api_key: bool = False,
        callback: Callable = None,
        max_fetches: int = 0,
        delay: float = DELAY_SECONDS,
    ) -> None:
        logger.info(f"calling async_list {url} {params}")
        json_data = await cls.async_list(url, params, include_api_key=include_api_key)
        logger.debug(f"Got for {url} : json_data={json_data}")
        callback(json_data["data"])
        fetches = 1

        while has_next_url(json_data):
            if max_fetches > 0 and fetches >= max_fetches:
                break
            await asyncio.sleep(delay)
            logger.info(f"calling async_next {url} {params}")
            json_data = await cls.async_next(json_data["meta"]["next"], params, include_api_key=include_api_key)
            logger.debug(f"Got for {url} : json_data={json_data}")
            callback(json_data["data"])
            fetches += 1
