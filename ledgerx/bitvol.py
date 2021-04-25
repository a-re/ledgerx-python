from typing import List, Dict
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_url
import datetime as dt

import logging

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
        res = HttpClient.get(url, params, include_api_key)
        return res.json()

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

class BitvolCache:
    cache = dict() # dict(asset : latest_bitvol_json)
    timezone = dt.timezone.utc
    timefmt = "%Y-%m-%dT%H:%M:%S%z"
    timefmt_ms = "%Y-%m-%dT%H:%M:%S.%f%z"

    @classmethod
    def get_bitvol(cls, asset, resolution = "1W", timeout = 120):
        if asset == "CBTC":
            asset = "BTC"
        bitvol = None
        key = "-".join([asset, resolution])
        now = dt.datetime.now(cls.timezone)
        if key in cls.cache:
            bitvol = cls.cache[key]
        if bitvol is not None:
            then = bitvol['time']
            if '.' in then:
                then = dt.datetime.strptime(then, cls.timefmt_ms)
            else:
                then = dt.datetime.strptime(then, cls.timefmt)
            if (now - then).total_seconds() > timeout:
                bitvol = None
        if bitvol is None:
            bitvol_results = Bitvol.list(dict(asset=asset, resolution=resolution))
            logging.info(f"bitvol list({asset}, {resolution}) returned {bitvol_results}")
            bitvol = bitvol_results['data'][-1]
            cls.cache[key] = bitvol
        return bitvol['value']

