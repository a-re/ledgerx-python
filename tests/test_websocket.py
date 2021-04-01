import ledgerx
import asyncio
from time import sleep
import logging


from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

def test_methods():
    class_methods = dir(ledgerx.WebSocket)
    assert "connect" in class_methods
    assert "listen" in class_methods
    assert "consumer_handle" in class_methods
    assert "send" in class_methods
    assert "close" in class_methods


def test_connect():
    logging.info("test_connect")
    websocket = ledgerx.WebSocket()
    websocket.connect(False)
    logging.info("test_connect connected")
    return websocket


async def listen(websocket):
    await websocket.listen()

async def delay_terminate(websocket):
    task = asyncio.sleep(3)
    logging.info("delay_termination sleeping")
    await task
    logging.info("delay_termination closing")
    await websocket.close()
    logging.info("delay_termination closed")


async def async_test_listen():
    ws = test_connect()
    task1 = asyncio.create_task(listen(ws))
    task2 = asyncio.create_task(delay_terminate(ws))
    await asyncio.gather(task1, task2)

def test_listen():
    asyncio.run(async_test_listen())
