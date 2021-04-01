import asyncio
import logging
import websockets
import json

from ledgerx.util import gen_websocket_url

logging.basicConfig(level=logging.INFO)

class WebSocket:

    connection = None
        
    def connect(self, include_api_key : bool = False) -> websockets.client.WebSocketClientProtocol:
        websocket_resource_url = gen_websocket_url(include_api_key)
        logging.info(f"Connecting to {websocket_resource_url}")
        self.connection = websockets.connect(websocket_resource_url)
        logging.info(f"Connected {self.connection}")

    async def close(self):
        logging.info(f"Closing connection {self.connection}")
        async with self.connection as websocket:
            await websocket.close()
        self.connection = None
        logging.info(f"Closed connection {self.connection}")

    def update_book_top(self, data):
        logging.info(f"Received: {data}")

    def update_by_type(self, data):
        type = data['type']
        if type == 'book_top':
            self.update_book_top(data)
        elif type == 'unauth_success':
            logging.info("Successful unauth connection")
        else:
            logging.warn(f"Unknown type '{type}': {data}")

    async def consumer_handle(self, websocket: websockets.client.WebSocketClientProtocol) -> None:
        logging.info(f"consumer_handle starting: {websocket}")
        async for message in websocket:
            #logging.info(f"Received: {message}")
            data = json.loads(message)
            if 'type' in data:
                self.update_by_type(data)
            elif 'error' in data:
                logging.warn(f"Got an error: {message}")
                break
            else:
                logging.warn(f"Got unexpected message: {message}")
            if self.connection is None:
                break
        logging.info(f"consumer_handle exited: {websocket}")

    async def listen(self):
        logging.info(f"listening to websocket: {self.connection}")
        async with self.connection as websocket:
            logging.info(f"...{websocket}")
            #await asyncio.wait_for(self.consumer_handle(websocket), timeout=5)
            await self.consumer_handle(websocket)
        logging.info(f"stopped listening to websocket: {self.connection}")

    def listen_loop(self):
        asyncio.run_until_complete(self.listen())
    
    async def send(self, message: str) -> None:
        logging.info(f"Sending: {message}")
        await self.connection.send(message)


    
