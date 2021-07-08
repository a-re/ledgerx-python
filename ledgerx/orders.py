from typing import Dict
import asyncio
import requests
import aiohttp
import logging

from requests.models import HTTPError
from ledgerx.http_client import HttpClient
from ledgerx.util import gen_legacy_url


class Orders:
    default_list_params = dict()

    @classmethod
    def cancel_all(cls) -> Dict:
        """Delete all outstanding orders associated with your MPID (the whole organization)

        https://docs.ledgerx.com/reference#cancel-all

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url("/orders")
        res = HttpClient.delete(url, {}, include_api_key)
        return res.json()

    @classmethod
    def cancel_single(cls, mid: str, contract_id: int) -> Dict:
        """Cancel a single resting limit order

        https://docs.ledgerx.com/reference#cancel-single

        Args:
            mid (str): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders/{mid}")
        qps = dict(contract_id=contract_id)
        try:
            res = HttpClient.delete(url, qps, include_api_key)
            return res.json()
        except requests.HTTPError as e:
            if e.response.status_code == 400:
                logging.info(f"Looks like {mid} is already cancelled: {e}")
                pass
            else:
                logging.exception(f"Could not cancel {mid} on {contract_id}. {e}")
                raise
        return None

    @classmethod
    def create(cls, contract_id: int, is_ask: bool, size: int, price: int, volatile: bool = False, order_type: str = 'limit', swap_purpose: str = 'undisclosed') -> Dict:
        """Create a new resting limit order.

        
        https://docs.ledgerx.com/reference#create-order

        Args:
            mid (str): [description]
            contract_id (int): [description]
            price (int): [description]
            size (int): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders")
        qps = dict(order_type=order_type, contract_id=contract_id, is_ask='true' if is_ask else 'false', swap_purpose=swap_purpose, size=size, price=price, volatile=True if volatile else False)
        res = HttpClient.post(url, qps, include_api_key)
        return res.json()

    @classmethod
    def cancel_replace(cls, mid: str, contract_id: int, price: int, size: int) -> Dict:
        """Atomically swap an existing resting limit order with a new resting limit order. Price, side and size may be changed.

        Rate Limit Notice: This endpoint has a rate limit of 500 requests per 10 seconds.

        https://docs.ledgerx.com/reference#cancel-replace

        Args:
            mid (str): [description]
            contract_id (int): [description]
            price (int): [description]
            size (int): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders/{mid}/edit")
        qps = dict(contract_id=contract_id, price=price, size=size)
        res = HttpClient.post(url, qps, include_api_key)
        return res.json()

    @classmethod
    def list_open(cls, params: Dict = {}) -> Dict:
        """Get all resting limit orders directly from the exchange

        https://docs.ledgerx.com/reference#open-orders

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url("/open-orders")
        res = HttpClient.get(url, {}, include_api_key)
        return res.json()

    @classmethod
    async def async_cancel_single(cls, mid: str, contract_id: int) -> Dict:
        """Cancel a single resting limit order

        https://docs.ledgerx.com/reference#cancel-single

        Args:
            mid (str): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders/{mid}")
        qps = dict(contract_id=contract_id)
        try:
            res = await HttpClient.async_delete(url, qps, include_api_key)
            return await res.json()
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 400:
                logging.info(f"Looks like {mid} is already cancelled: {e}")
                pass
            else:
                logging.exception(f"Failed to cancel {mid} on {contract_id}... perhaps it no longer exists? exception={e}")
                raise
        return None
        

    @classmethod
    async def async_create(cls, contract_id: int, is_ask: bool, size: int, price: int, volatile: bool = False, order_type: str = 'limit', swap_purpose: str = 'undisclosed') -> Dict:
        """Create a new resting limit order.

        
        https://docs.ledgerx.com/reference#create-order

        Args:
            mid (str): [description]
            contract_id (int): [description]
            price (int): [description]
            size (int): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders")
        qps = dict(order_type=order_type, contract_id=contract_id, is_ask='true' if is_ask else 'false', swap_purpose=swap_purpose, size=size, price=price, volatile='true' if volatile else 'false')
        res = await HttpClient.async_post(url, qps, include_api_key)
        return await res.json()



    @classmethod
    async def async_cancel_replace(cls, mid: str, contract_id: int, price: int, size: int) -> Dict:
        """Atomically swap an existing resting limit order with a new resting limit order. Price, side and size may be changed.

        Rate Limit Notice: This endpoint has a rate limit of 500 requests per 10 seconds.

        https://docs.ledgerx.com/reference#cancel-replace

        Args:
            mid (str): [description]
            contract_id (int): [description]
            price (int): [description]
            size (int): [description]

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url(f"/orders/{mid}/edit")
        qps = dict(contract_id=contract_id, price=price, size=size)
        res = await HttpClient.async_post(url, qps, include_api_key)
        return await res.json()

    @classmethod
    async def async_list_open(cls, params: Dict = {}) -> Dict:
        """Get all resting limit orders directly from the exchange

        https://docs.ledgerx.com/reference#open-orders

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            Dict: [description]
        """
        include_api_key = True
        url = gen_legacy_url("/open-orders")
        res = await HttpClient.async_get(url, {}, include_api_key)
        return await res.json()
