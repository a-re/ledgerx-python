import asyncio
from typing import List, Dict
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_url
import datetime as dt
import time

import logging

logger = logging.getLogger(__name__)

class Bitvol:
    @classmethod
    def list(cls, params: Dict = {}) -> List[Dict]:
        """Get bitvol data

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: bitvol objects
        """
        include_api_key = True
        url = gen_url("/trading/bitvol")
        try:
            res = HttpClient.get(url, params, include_api_key)
        except:
            logger.warning(f"Could not get bitvol for {params}")
            return None
        return res.json()

    @classmethod
    async def async_list(cls, params: Dict = {}) -> List[Dict]:
        """Get bitvol data

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: bitvol objects
        """
        include_api_key = True
        url = gen_url("/trading/bitvol")
        try:
            res = await HttpClient.async_get(url, params, include_api_key)
        except:
            logger.warning(f"Could not get bitvol for {params}")
            return None
        return await res.json()

    ### Helper methods

    @classmethod
    def list_btc(cls, params: Dict = {}) -> List[Dict]:
        """Fetch BTC bitvol data

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
        """Fetch ETH bitvol data

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
        """Fetch BTC bitvol data

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
        """Fetch ETH bitvol data

        Args:
            params (Dict): [description]

        Returns:
            List[Dict]: [description]
        """
        default_params = {"asset": "ETH", "resolution": "1W"}
        qps = {**default_params, **params}
        return await cls.async_list(qps)

class BitvolCache:
    cache = dict() # dict(asset : latest_bitvol_json)
    timezone = dt.timezone.utc
    timefmt = "%Y-%m-%dT%H:%M:%S%z"
    timefmt_ms = "%Y-%m-%dT%H:%M:%S.%f%z"

    @classmethod
    def to_time(cls, then):
        try:
            then = dt.datetime.fromisoformat(then)
        except:
            logger.warning(f"Did not parse isoformat of {then} trying {cls.timefmt}")
            if '.' in then:
                then = dt.datetime.strptime(then, cls.timefmt_ms)
            else:
                then = dt.datetime.strptime(then, cls.timefmt)
        return then

    @classmethod
    def get_cached_bitvol(cls, asset, resolution = "1W", timeout = 3750):
        """Returns the cached value and None if the cache is empty or out of date"""
        if asset == "CBTC":
            asset = "BTC"
        if asset == "USD":
            raise RuntimeError("No bit vol for USD")
        bitvol = None
        key = "-".join([asset, resolution])
        now = dt.datetime.now(cls.timezone)
        if key in cls.cache:
            bitvol = cls.cache[key]
        else:
            logger.info(f"No cache for {key} {cls.cache}")
        if bitvol is not None:
            then = cls.to_time(bitvol['time'])
            if timeout is not None and (now - then).total_seconds() > timeout:
                logger.warning(f"Cache entry for {key} is too old {then} vs {now} forcing reload")
                bitvol = None
                bitvol_results = Bitvol.list(dict(asset=asset, resolution=resolution))
                logger.info(f"Got bitvol results for {asset} {resolution}: {bitvol_results}")
                cls.store_cached_results(asset, resolution, bitvol_results)
                newbitvol = cls.cache[key]
                newthen = cls.to_time(newbitvol['time'])
                if newthen == then:
                    logger.warning(f"Bitvol is stale, updating to this hour")
                    newbitvol['time'] = dt.datetime.isoformat(now)
                    bitvol = newbitvol
        return bitvol
    
    @classmethod
    def update_cached_bitvol(cls, ws_data):
        assert('type' in ws_data and 'asset' in ws_data)
        if ws_data['value'] is not None and ws_data['time'] is not None:
            asset = ws_data['asset']
            now = cls.to_time(ws_data['time'])
            keys = cls.cache.keys()
            for key in keys:
                if asset in key:
                    bitvol = cls.cache[key]
                    then = cls.to_time(bitvol['time'])
                    if now > then:
                        cls.cache[key] = ws_data
    
    @classmethod
    def store_cached_results(cls, asset, resolution, bitvol_results):
        bitvol = None
        if bitvol_results is None:
            return bitvol
        logger.info(f"bitvol list({asset}, {resolution}) returned keys={bitvol_results.keys()} ['data']={len(bitvol_results['data'])}")
        if asset == "CBTC":
            asset = "BTC"
        elif asset == "CETH":
            asset = "ETH"
        key = "-".join([asset, resolution])
        for result in reversed(bitvol_results['data']):
            # test the result, sometimes a value comes back as None
            if result['value'] is not None and (bitvol is None or bitvol['time'] < result['time']):
                bitvol = result
                cls.cache[key] = bitvol
                logger.info(f"stored {key}={bitvol}")
                break
        logger.info(f"latest bitvol={bitvol}")
        return bitvol

    getting_bitvol = dict()
    @classmethod
    def get_bitvol(cls, asset, resolution = "1W", timeout = 3750):
        bitvol = cls.get_cached_bitvol(asset, resolution, timeout)
        if bitvol is None:
            key = "-".join([asset, resolution])
            now = dt.datetime.now(cls.timezone)
            if key in cls.getting_bitvol and (now - cls.getting_bitvol[key]).total_seconds() < 30:
                bitvol = cls.get_cached_bitvol(asset, resolution, None)
            else:
                cls.getting_bitvol[key] = now
                bitvol_results = Bitvol.list(dict(asset=asset, resolution=resolution))
                logger.info(f"Got bitvol results for {asset} {resolution}: {bitvol_results}")
                bitvol = cls.store_cached_results(asset, resolution, bitvol_results)
        return None if bitvol is None else bitvol['value']

    @classmethod
    async def async_get_bitvol(cls, asset, resolution = "1W", timeout = 3750):
        logger.info(f"Getting bitvol for {asset}")
        bitvol = cls.get_cached_bitvol(asset, resolution, timeout)
        if bitvol is None:
            key = "-".join([asset, resolution])
            now = dt.datetime.now(cls.timezone)
            if key in cls.getting_bitvol and (now - cls.getting_bitvol[key]).total_seconds() < 30:
                bitvol = cls.get_cached_bitvol(asset, resolution, None)
            else:
                cls.getting_bitvol[key] = now
                bitvol_results = await Bitvol.async_list(dict(asset=asset, resolution=resolution))
                bitvol = cls.store_cached_results(asset, resolution, bitvol_results)
        return None if bitvol is None else bitvol['value']

      
        

