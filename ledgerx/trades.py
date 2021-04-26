from typing import List, Dict, Callable
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_url
from ledgerx.generic_resource import GenericResource
from ledgerx import DEFAULT_LIMIT
from ledgerx import DELAY_SECONDS


class Trades:
    default_list_params = dict(
        status_type=201, limit=DEFAULT_LIMIT, min_size=1, mine=False, asset="CBTC"
    )
    default_list_all_params = dict(limit=DEFAULT_LIMIT)
    default_trading_trades_global_delay = DELAY_SECONDS

    @classmethod
    def list(cls, params: Dict = {}) -> List[Dict]:
        """Returns a list of your trades.

        https://docs.ledgerx.com/reference#listtrades

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: [description]
        """
        include_api_key = True
        url = gen_url("/trading/trades")
        request_params = {**cls.default_list_params, **params}
        res = HttpClient.get(url, request_params, include_api_key)
        data = res.json()
        return data

    @classmethod
    def list_all(cls, params: Dict = {}) -> List[Dict]:
        """Returns a list of all trades in the market.

        https://docs.ledgerx.com/reference#globalstrade

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: [description]
        """
        include_api_key = False
        url = gen_url("/trading/trades/global")
        request_params = {**cls.default_list_all_params, **params}
        return GenericResource.list_all(url, request_params, include_api_key, 0, cls.default_trading_trades_global_delay)

    # helper methods specific to this API client

    @classmethod
    def list_all_incremental_return(cls, params: Dict = {}, callback: Callable = None):
        # """List all trades and execute callback function after
        # each HTTP request (ie, in between pagination breaks).

        # This API request calls the Trades.list_all function.

        # See Trades.list_all for more info.

        # Args:
        #     params (Dict, optional): [description]. Defaults to {}.
        #     callback (Callable, optional): [description]. Defaults to None.

        # Returns:
        #     [type]: [description]
        # """
        include_api_key = False
        url = gen_url("/trading/trades/global")
        request_params = {**cls.default_list_params, **params}
        return GenericResource.list_all_incremental_return(
            url, params, include_api_key, callback, 0, cls.default_trading_trades_global_delay
        )

    @classmethod
    def next(cls, next_url: str):
        res = HttpClient.get(next_url)
        return res.json()

    @classmethod
    async def async_list_all(cls, params: Dict = {}) -> List[Dict]:
        """Returns a list of all trades in the market.

        https://docs.ledgerx.com/reference#globalstrade

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: [description]
        """
        include_api_key = False
        url = gen_url("/trading/trades/global")
        request_params = {**cls.default_list_all_params, **params}
        return await GenericResource.async_list_all(url, request_params, include_api_key, 0, cls.default_trading_trades_global_delay)


    @classmethod
    async def async_list_all_incremental_return(cls, params: Dict = {}, callback: Callable = None):
        # """List all trades and execute callback function after
        # each HTTP request (ie, in between pagination breaks).

        # This API request calls the Trades.list_all function.

        # See Trades.list_all for more info.

        # Args:
        #     params (Dict, optional): [description]. Defaults to {}.
        #     callback (Callable, optional): [description]. Defaults to None.

        # Returns:
        #     [type]: [description]
        # """
        include_api_key = False
        url = gen_url("/trading/trades/global")
        request_params = {**cls.default_list_params, **params}
        return await GenericResource.async_list_all_incremental_return(
            url, params, include_api_key, callback, 0, cls.default_trading_trades_global_delay
        )
