from typing import Dict
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_legacy_url


class BookStates:
    default_list_params = dict()

    @classmethod
    def get_book_states(cls, contract_id: int) -> Dict:
        """Queries 

        https://docs.ledgerx.com/reference#book-states

        Args:
            contract_id (int): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/book-states/{contract_id}")
        qps = dict(**cls.default_list_params)
        res = HttpClient.get(url, qps, include_api_key)
        return res.json()["data"]
