import asyncio
import socketio

sio = socketio.AsyncClient()


@sio.event
async def connect():
    print('connected to server')


@sio.event
async def disconnect():
    print('disconnected from server')


@sio.event
def last_traded_price(data):
    print('last_traded_price {} '.format(data))

@sio.event
def intraday_prices(data):
    print('intraday_prices {} '.format(data))

@sio.event
def intraday_trending_stocks(data):
    print('intraday_trending_stocks {} '.format(data))


async def start_server():
    #await sio.connect('http://localhost:5000', wait_timeout=10000000 * 999999999, wait=True)
    await sio.connect('http://ec2-18-224-151-229.us-east-2.compute.amazonaws.com:5000')

    await sio.wait()


if __name__ == '__main__':
    asyncio.run(start_server())