import asyncio
import socketio
from tools.logger import logger_rtapic
sio = socketio.AsyncClient()


@sio.event
async def connect():
    logger_rtapic.info('connected to server')


@sio.event
async def disconnect():
    logger_rtapic.info('disconnected from server')


@sio.event
def last_traded_price(data):
    logger_rtapic.info('last_traded_price {} '.format(data))

@sio.event
def intraday_prices(data):
    logger_rtapic.info('intraday_prices {} '.format(data))

@sio.event
def intraday_trending_stocks(data):
    logger_rtapic.info('intraday_trending_stocks {} '.format(data))


async def start_server():
    #await sio.connect('http://localhost:5000', wait_timeout=10000000 * 999999999, wait=True)
    await sio.connect('http://52.14.177.160:5000')

    await sio.wait()


if __name__ == '__main__':
    asyncio.run(start_server())