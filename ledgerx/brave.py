import asyncio
from typing import List, Dict
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_url
import datetime as dt

import logging

logger = logging.getLogger(__name__)

class Brave:
    @classmethod
    def list(cls, params: Dict = {}) -> List[Dict]:
        """Get brave data

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: brave objects
        """
        include_api_key = True
        url = gen_url("/trading/brave")
        res = HttpClient.get(url, params, include_api_key)
        return res.json()

    @classmethod
    async def async_list(cls, params: Dict = {}) -> List[Dict]:
        """Get brave data

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: brave objects
        """
        include_api_key = True
        url = gen_url("/trading/brave")
        res = await HttpClient.async_get(url, params, include_api_key)
        return await res.json()

    ### Helper methods

    @classmethod
    def list_btc(cls, params: Dict = {}) -> List[Dict]:
        """Fetch BTC brave data

        Args:
            params (Dict): [description]

        Returns:
            List[Dict]: [description]
        """
        default_params = {"asset": "BTC", "resolution": "1W"}
        qps = {**default_params, **params}
        return cls.list(qps)

    @classmethod
    def list_eth(cls, params: Dict = {}) -> List[Dict]:
        """Fetch ETH brave data

        Args:
            params (Dict): [description]

        Returns:
            List[Dict]: [description]
        """
        default_params = {"asset": "ETH", "resolution": "1W"}
        qps = {**default_params, **params}
        return cls.list(qps)

    @classmethod
    async def async_list_btc(cls, params: Dict = {}) -> List[Dict]:
        """Fetch BTC brave data

        Args:
            params (Dict): [description]

        Returns:
            List[Dict]: [description]
        """
        default_params = {"asset": "BTC", "resolution": "1W"}
        qps = {**default_params, **params}
        return await cls.async_list(qps)

    @classmethod
    async def async_list_eth(cls, params: Dict = {}) -> List[Dict]:
        """Fetch ETH brave data

        Args:
            params (Dict): [description]

        Returns:
            List[Dict]: [description]
        """
        default_params = {"asset": "ETH", "resolution": "1W"}
        qps = {**default_params, **params}
        return await cls.async_list(qps)

class BraveCache:
    cache = dict() # dict(asset : latest_brave_json)
    timezone = dt.timezone.utc
    timefmt = "%Y-%m-%dT%H:%M:%S%z"
    timefmt_ms = "%Y-%m-%dT%H:%M:%S.%f%z"

    @classmethod
    def to_time(cls, then):
        if '.' in then:
            then = dt.datetime.strptime(then, cls.timefmt_ms)
        else:
            then = dt.datetime.strptime(then, cls.timefmt)
        return then

    @classmethod
    def get_cached_brave(cls, asset, resolution = "1W", timeout = 120):
        """Returns the cached value and None if the cache is empty or out of date"""
        if asset == "CBTC":
            asset = "BTC"
        if asset == "USD":
            raise RuntimeError("No bit vol for USD")
        brave = None
        key = "-".join([asset, resolution])
        now = dt.datetime.now(cls.timezone)
        if key in cls.cache:
            brave = cls.cache[key]
        else:
            logger.info(f"No cache for {key} {cls.cache}")
        if brave is not None:
            then = cls.to_time(brave['time'])
            if (now - then).total_seconds() > timeout:
                brave = None
        return brave
    
    @classmethod
    def update_cached_brave(cls, ws_data):
        assert('type' in ws_data and 'asset' in ws_data)
        if ws_data['value'] is not None and ws_data['time'] is not None:
            asset = ws_data['asset']
            now = cls.to_time(ws_data['time'])
            keys = cls.cache.keys()
            for key in keys:
                if asset in key:
                    brave = cls.cache[key]
                    then = cls.to_time(brave['time'])
                    if now > then:
                        cls.cache[key] = ws_data
    
    @classmethod
    def store_cached_results(cls, asset, resolution, brave_results):
        logger.info(f"brave list({asset}, {resolution}) returned keys={brave_results.keys()} ['data']={brave_results['data']}")
        brave = None
        if asset == "CBTC":
            asset = "BTC"
        elif asset == "CETH":
            asset = "ETH"
        key = "-".join([asset, resolution])
        for result in reversed(brave_results['data']):
            # test the result, sometimes a price comes back as None
            if result['price'] is not None and (brave is None or brave['time'] < result['time']):
                brave = result
                cls.cache[key] = brave
                logger.info(f"stored {key}={brave}")
                break
        logger.info(f"latest brave={brave}")
        return brave

    @classmethod
    def get_brave(cls, asset, resolution = "1W", timeout = 120):
        brave = cls.get_cached_brave(asset, resolution, timeout)
        if brave is None:
            brave_results = Brave.list(dict(asset=asset, resolution=resolution))
            brave = cls.store_cached_results(asset, resolution, brave_results)
        return brave

    @classmethod
    async def async_get_brave(cls, asset, resolution = "1W", timeout = 120):
        brave = cls.get_cached_brave(asset, resolution, timeout)
        if brave is None:
            brave_results = await Brave.async_list(dict(asset=asset, resolution=resolution))
            brave = cls.store_cached_results(asset, resolution, brave_results)
        return brave

      
        

