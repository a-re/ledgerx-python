from typing import List, Dict
from ledgerx.http_client import HttpClient
from ledgerx.generic_resource import GenericResource
from ledgerx.util import gen_url
from ledgerx import DEFAULT_LIMIT


class Positions:
    default_positions_params = dict()
    default_trades_params = dict()

    @classmethod
    def list(cls, params: Dict = dict(limit=DEFAULT_LIMIT)) -> List[Dict]:
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
    def list_trades(cls, position_id: int, params: Dict = dict(limit=DEFAULT_LIMIT)) -> Dict:
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

    @classmethod  # FIXME pagination is broken for positions
    def list_all(cls, params: Dict = dict(limit=DEFAULT_LIMIT*100)) -> List[Dict]:
        include_api_key = True
        url = gen_url("/trading/positions")
        qps = {**cls.default_positions_params, **params}
        return GenericResource.list_all(url, qps, include_api_key)

    @classmethod  # FIXME pagination is broken for positions
    def list_all_trades(cls, position_id: int, params: Dict = dict(limit=DEFAULT_LIMIT*100)) -> Dict:
        include_api_key = True
        url = gen_url(f"/trading/positions/{position_id}/trades")
        qps = {**cls.default_trades_params, **params}
        return GenericResource.list_all(url, qps, include_api_key)
