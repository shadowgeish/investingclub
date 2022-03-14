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
def new_orders(data):
    logger_rtapic.info('new_orders {}'.format(data))

@sio.event
def new_trades(data):
    logger_rtapic.info('new_trades {}'.format(data))

@sio.event
def best_bid_ask(data):
    logger_rtapic.info('best_bid_ask {}'.format(data))

@sio.event
def intraday_trending_stocks(data):
    logger_rtapic.info('intraday_trending_stocks {} '.format(data))


async def start_server():
    await sio.connect('http://localhost:5005')
    #await sio.connect('http://18.191.227.200:5000')

    await sio.wait()


if __name__ == '__main__':
    asyncio.run(start_server())