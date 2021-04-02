from typing import List, Dict
from ledgerx.http_client import HttpClient
from ledgerx.generic_resource import GenericResource
from ledgerx.util import gen_url
from ledgerx import DEFAULT_LIMIT


class Positions:
    default_positions_params = dict(limit=DEFAULT_LIMIT)
    default_trades_params = dict(limit=DEFAULT_LIMIT)

    @classmethod
    def list(cls, params: Dict = dict()) -> List[Dict]:
        """Returns all your positions.

        https://docs.ledgerx.com/reference#listpositions

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: [description]
        """
        include_api_key = True
        url = gen_url("/trading/positions")
        qps = {**cls.default_positions_params, **params}
        res = HttpClient.get(url, qps, include_api_key)
        return res.json()

    @classmethod
    def list_trades(cls, position_id: int, params: Dict = dict()) -> Dict:
        """Returns a list of your trades for a given position.

        Args:
            position_id (int): LedgerX position ID.

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_url(f"/trading/positions/{position_id}/trades")
        qps = {**cls.default_trades_params, **params}
        res = HttpClient.get(url, qps, include_api_key)
        return res.json()

    ### helper methods specific to this API client

    @classmethod
    def list_all(cls, params: Dict = dict()) -> List[Dict]:
        include_api_key = True
        url = gen_url("/trading/positions")
        qps = {**cls.default_positions_params, **params}
        return GenericResource.list_all(url, qps, include_api_key)

    @classmethod
    def list_all_trades(cls, position_id: int, params: Dict = dict()) -> Dict:
        include_api_key = True
        url = gen_url(f"/trading/positions/{position_id}/trades")
        qps = {**cls.default_trades_params, **params}
        return GenericResource.list_all(url, qps, include_api_key)
