import asyncio
import logging
import websockets
import json

from ledgerx.util import gen_websocket_url
from ledgerx.market_state import MarketState

class WebSocket:

    connection = None
    market_state = None
    run_id = None
    heartbeat = None
        
    def connect(self, include_api_key : bool = False) -> websockets.client.WebSocketClientProtocol:
        websocket_resource_url = gen_websocket_url(include_api_key)
        logging.debug(f"Connecting to {websocket_resource_url}")
        self.connection = websockets.connect(websocket_resource_url)
        logging.info(f"Connected {self.connection}")

    async def close(self):
        logging.debug(f"Closing connection {self.connection}")
        async with self.connection as websocket:
            await websocket.close()
        self.connection = None
        logging.info(f"Closed connection {self.connection}")

    def update_book_top(self, data):
        logging.debug(f"book_top: {data}")

    def update_heartbeat(self, data):
        logging.debug(f"heartbeat: {data}")
        if self.heartbeat is None:
            # first one
            self.heartbeat = data
        else:
            if self.heartbeat['ticks'] + 1 != data['ticks']:
                diff = data['ticks'] - self.heartbeat['ticks'] - 1
                logging.info(f"Missed {diff} heartbeats {self.heartbeat} vs {data}")
            if self.heartbeat['run_id'] != data['run_id']:
                logging.warn("Detected a restart!")
            self.heartbeat = data

    def update_by_type(self, data):
        type = data['type']
        if type == 'book_top':
            self.update_book_top(data)
        elif type == 'heartbeat':
            self.update_heartbeat(data)
        elif type == 'unauth_success':
            logging.info("Successful unauth connection")
        elif type == 'auth_success':
            logging.info("Successful auth connection")
        elif type == 'collateral_balance_update':
            logging.debug(f"Collateral balance {data}")
        elif type == 'open_positions_update':
            logging.debug(f"Open Positions {data}")
        elif type == 'exposure_reports':
            logging.debug(f"Exposure reports {data} ")
        elif type == 'action_report':
            logging.debug(f"action report {data}")
        else:
            logging.warn(f"Unknown type '{type}': {data}")

        if self.market_state is not None:
            self.market_state.handle_action(data)

    async def consumer_handle(self, websocket: websockets.client.WebSocketClientProtocol) -> None:
        logging.info(f"consumer_handle starting: {websocket}")
        async for message in websocket:
            logging.debug(f"Received: {message}")
            data = json.loads(message)
            if 'type' in data:
                self.update_by_type(data)
            elif 'error' in data:
                logging.warn(f"Got an error: {message}")
                break
            else:
                logging.warn(f"Got unexpected message: {message}")
            if self.connection is None:
                logging.info("Connection is gone")
                break
        logging.info(f"consumer_handle exited: {websocket}")

    async def listen(self):
        logging.info(f"listening to websocket: {self.connection}")
        async with self.connection as websocket:
            logging.info(f"...{websocket}")
            await self.consumer_handle(websocket)
        logging.info(f"stopped listening to websocket: {self.connection}")

    
    async def send(self, message: str) -> None:
        logging.info(f"Sending: {message}")
        await self.connection.send(message)


    
