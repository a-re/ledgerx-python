import asyncio
import concurrent.futures
import logging
import websockets
import json
from time import sleep
from multiprocessing import AuthenticationError
from multiprocessing.connection import Listener
import datetime as dt

from ledgerx.util import gen_websocket_url
import ledgerx

class WebSocket:

    websocket = None
    #connection = None
    #update_callbacks = list()
    #run_id = None
    #heartbeat = None
    localhost_connections = []
    include_api_key = False
    active = True
    repeat_server = None

    def __init__(self):
        self.clear()

    def clear(self):
        self.connection = None
        self.update_callbacks = list()
        #self.run_id = None
        self.heartbeat = None
        self.apikey = None
        self.include_api_key = False
        for conn in self.localhost_connections:
            try:
                conn.close()
            except:
                logging.exception(f"Could not close {conn}")
        self.localhost_connections = []

    def register_callback(self, callback):
        """A call back will get called for every message with a 'type' field"""
        for cb in self.update_callbacks:
            if cb == callback:
                logging.warn(f"Attempt to register a callback twice. {cb} {callback}... Okay then, continuing.")
        self.update_callbacks.append(callback)
        logging.info(f"Registered callback {callback}, now there are {len(self.update_callbacks)}")
        

    def deregister_callback(self, callback):
        self.update_callbacks.remove(callback)
        logging.info(f"Deregistered callback {callback}, now there are {len(self.update_callbacks)}")
        
    def connect(self, include_api_key : bool = False) -> websockets.client.WebSocketClientProtocol:
        websocket_resource_url = gen_websocket_url(include_api_key)
        self.include_api_key = include_api_key
        logging.debug(f"Connecting to {websocket_resource_url}")
        self.connection = websockets.connect(websocket_resource_url)
        logging.info(f"Connected connection={self.connection} include_api_key={include_api_key}")

    async def close(self):
        logging.info(f"Closing connection {self.connection}")
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
                if diff >= 5:
                    logging.warn(f"Missed {diff} heartbeats {self.heartbeat} vs {data}")
                else:
                    logging.debug(f"Missed {diff} heartbeats {self.heartbeat} vs {data}")
            if self.heartbeat['run_id'] != data['run_id']:
                logging.warn("Detected a restart!")
            self.heartbeat = data

    async def update_by_type(self, data):
        type = data['type']
        if type == 'book_top':
            self.update_book_top(data)
        elif type == 'heartbeat':
            self.update_heartbeat(data)
        elif type == 'action_report':
            logging.debug(f"action report {data}")
        elif type == 'collateral_balance_update':
            logging.debug(f"Collateral balance {data}")
        elif type == 'open_positions_update':
            logging.debug(f"Open Positions {data}")
        elif type == 'exposure_reports':
            logging.debug(f"Exposure reports {data} ")
        elif type == 'contract_added':
            logging.debug(f"contract added {data}")
        elif type == 'unauth_success':
            logging.info("Successful unauth connection")
        elif type == 'auth_success':
            logging.info("Successful auth connection")            
        elif 'contact_' in type:
            logging.info(f"contact change {data}")
        elif 'conversation_' in type:
            logging.info(f"conversation change {data}")
        elif type == 'websocket_starting':
            logging.info(f"Websocket just started {data}")
        elif type == 'websocket_exception':
            logging.warn(f"websocket_exception {data}")
        elif type == 'subscribe':
            logging.info(f"subscribed: {data}")
        elif type == 'unsubscribe':
            logging.info(f"unsubscribe: {data}")
        elif type == 'bitvol':
            logging.debug(f"bitvol {data}")
        else:
            logging.warn(f"Unknown type '{type}': {data}")

        futures = []
        for callback in self.update_callbacks:
            if asyncio.iscoroutinefunction(callback):
                futures.append( callback(data) )
            else:
                callback(data)
        if len(futures) > 0:
            await asyncio.gather(*futures)

    async def consumer_handle(self, websocket: websockets.client.WebSocketClientProtocol) -> None:
        logging.info(f"consumer_handle starting: {websocket}")
        async for message in websocket:
            if not WebSocket.active:
                logging.info(f"WebSocket is no longer active")
                return
            logging.debug(f"Received: {message}")
            data = json.loads(message)
            if 'type' in data:
                await self.update_by_type(data)
            elif 'error' in data:
                logging.warn(f"Got an error: {message}")
                break
            else:
                logging.warn(f"Got unexpected message: {message}")
            if self.connection is None:
                logging.info("Connection is gone")
                break
        if self.connection is not None:
            logging.error(f"consumer_handle exited: websocket={websocket} connection={self.connection}")
            raise RuntimeError(f"websocket connection exited but it is not None {self.connection}")

    async def listen(self):
        logging.info(f"listening to websocket: {self.connection}")
        async with self.connection as websocket:
            logging.info(f"...{websocket}")
            await self.subscribe(websocket, ['btc_bitvol', 'eth_bitvol'])
            await self.consumer_handle(websocket)
        if self.active:
            logging.error(f"stopped listening to websocket: {self.connection}")
            raise RuntimeError(f"websocket stopped listening {self.connection}")
        else:
            logging.info("Websocket is not active")
            await self.close()


    async def subscribe(self, websocket, channels):
        msg = json.dumps(dict(type="subscribe", channels=channels))
        #msg = f'{{"type":"subscribe","channels":["{channel}"]}}\n'
        logging.info(f"Sending subscribe to {channels} with msg={msg}")
        await websocket.send(msg)
        logging.info(f"Subscribed")

    async def ping_pong(self):
        if self.connection is None or not self.active:
            logging.warning(f"Cannot ping_pong an inactive WebSocket")
            return
        async with self.connection as websocket:
            logging.info(f"Sending ping")
            pong_waiter = await websocket.ping()
            logging.info(f"Sent ping: {pong_waiter}")
            await pong_waiter
            logging.info(f"got pong {pong_waiter}")

    def localhost_socket_repeater_callback(self, message):
        to_remove = []
        for writer in self.localhost_connections:
            if writer.is_closing():
                logging.info(f"Closing writer {writer}")
                to_remove.append(writer)
                continue
            try:
                writer.write(f"{message}\n".encode('utf8'))
            except:
                logging.exception(f"Could not send to {writer}, closing it")
                to_remove.append(writer)
        for writer in to_remove:
            try:
                writer.close()
            except:
                logging.warn(f"Could not close {writer}")
            self.localhost_connections.remove(writer)

    async def handle_localhost_socket(self, reader, writer):
        """
        The only incoming messages from reader should be the api_key, 
        all others besides 'quit' will be ignored
        """
        request = None
        is_auth = False
        needs_auth = self.include_api_key
        if not needs_auth:
            logging.info(f"No need for authentication")
            self.localhost_connections.append(writer)
        else:
            logging.info(f"Requiring authentication for repeat server")
        while request != "quit" and self.active:
            if writer.is_closing():
                logging.info("Dectected closing writer socket")
                break
            request = (await reader.read(512)).decode('utf8').rstrip()
            if needs_auth:
                if request != ledgerx.api_key:
                    logging.warn(f"Got incorrect api key...Closing connection")
                    writer.write("Invalid authentication\n".encode('utf8'))
                    await writer.drain()
                    break
                else:
                    needs_auth = False
                    logging.info(f"Successful Authentication")
                    self.localhost_connections.append(writer)
            else:
                if request == "":
                    logging.info("Detected closing of reader socket")
                    break
                logging.info(f"from localhost socket, got: {request}")
        if not self.active:
            logging.info("localhost socket is not active now")
        writer.close()

    @classmethod
    def disconnect(cls):
        logging.info("Signaling disconnect")
        cls.active = False
        if cls.repeat_server:
            cls.repeat_server.close()

    @classmethod
    async def run_server(cls, *callbacks, **kw_args) -> None:
        """
        starts, with asyncio, a server listening the the ledgerx websocket 
        if repeat_server_port is included, start repeating messages on the localhost:repeat_server_port
        if callbacks is provided, also register callbacks and repeat messages to them
        if include_api_key is True, the websocket will send ledgerx the api_key and repeater port will require it too upon connection

        Usage:
        asyncio.run(ledgerx.WebSocket.run_server([callbacks,], include_api_key=False, repeat_server_port=None))

        """
        logging.info(f"run_server with {kw_args} and {len(callbacks)} callbacks")
        if 'include_api_key' not in kw_args:
            cls.include_api_key = False
        else:
            cls.include_api_key = kw_args['include_api_key']
        if 'repeat_server_port' not in kw_args:
            kw_args['repeat_server_port'] = None
        
        run_iteration = 0
        while cls.active:
            cls.repeat_server = None
            run_iteration += 1
            try:
                loop = asyncio.get_running_loop()
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    
                    cls.websocket = WebSocket()
                    logging.info(f"Starting new WebSocket {cls.websocket}")
                    for callback in callbacks:
                        cls.websocket.register_callback(callback)
                    cls.websocket.register_callback(cls.websocket.localhost_socket_repeater_callback)
                    cls.websocket.connect(cls.include_api_key)

                    fut_notify = cls.websocket.update_by_type(dict(type="websocket_starting",\
                         data=dict(startup_time=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z"),\
                             run_iteration=run_iteration)))

                    
                    task1 = asyncio.create_task(cls.websocket.listen())
        
                    if kw_args['repeat_server_port'] is not None:
                        cls.repeat_server = await asyncio.start_server(cls.websocket.handle_localhost_socket, 'localhost',  kw_args['repeat_server_port'])
                        async with cls.repeat_server:
                            try:
                                await asyncio.gather(fut_notify, task1, cls.repeat_server.serve_forever())
                            except concurrent.futures._base.CancelledError:
                                logging.info("Repeat server was cancelled")
                                pass
                    else:
                        await asyncio.gather(fut_notify, task1)

                    logging.info(f"Websocket {cls.websocket} exited for some reason.")
            except:
                logging.exception(f"Got exception in websocket {cls.websocket}")
                cls.websocket = None
                futures = []
                for callback in callbacks:
                    fut = callback(dict(type="websocket_exception",\
                         data=dict(startup_time=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z"))))
                    futures.append(fut)
                if len(futures) > 0:
                    await asyncio.gather(*futures)
            if cls.active:
                logging.info("Continuing after 5 seconds")
                await asyncio.sleep(5)
                logging.info('Continuing...')
        logging.info(f"websocket run_server has concluded {cls}")

    