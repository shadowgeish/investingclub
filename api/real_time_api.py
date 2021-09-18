import asyncio


from sanic import Sanic
from sanic.response import html
from tools.logger import logger_rtapi

import socketio

sio = socketio.AsyncServer(async_mode='sanic')
app = Sanic(name='investing_club')
sio.attach(app)

real_time_price = dict()


async def index(request):
    return '<html></html>'
    #with open('app.html') as f:
    #    return web.Response(text=f.read(), content_type='text/html')


@app.listener('after_server_start')
def after_server_start(sanic, loop):
    sio.start_background_task(live_stock_prices)
    #sio.start_background_task(update_real_time_stock_prices_cache)
    return True


def is_serializable(obj):
    import json
    try:
        json.dumps(obj)
    except TypeError:
        logger_rtapi.error("Unable to serialize the object")
        return False

    return True


async def update_real_time_stock_prices_cache():
    from datetime import datetime
    from aiohttp import ClientSession
    import pytz
    import json
    import time
    import pandas as pd
    session = ClientSession()
    tz = pytz.timezone('Europe/Paris')
    paris_now = datetime.now(tz)

    start_date = datetime.strptime(paris_now.strftime("%d%m%Y0830%z"), '%d%m%Y%H%M%z')
    end_date = datetime.strptime(paris_now.strftime("%d%m%Y1830%z"), '%d%m%Y%H%M%z')
    dtt_s = start_date  # datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=8, minute=30, tzinfo=tz)
    dtt_e = end_date  # datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=18, minute=30, tzinfo=tz)
    first_run = True
    last_check_now = datetime.now(tz)
    while True:
        sec = (last_check_now - datetime.now(tz)).seconds
        if first_run is True or sec >= 10:
            first_run = False
            last_check_now = datetime.now(tz)
            server_run = '18.191.227.200' # localhost
            #server_run = 'localhost'  # localhost
            sreq = "http://{}:5001/api/v1/stock_universe_intraday_data/All?limit=5000&cache=false".format(server_run)
            logger_rtapi.info('Getting Real time save data {} '.format(sreq))

            async with session.get(sreq) as response:
                data = await response.read()
            try:
                import os.path
                stock_prices = json.loads(data)

                hdf = pd.HDFStore('real_time_prices.h5')

                #logger_rtapi.info('Received stock data to send {} '.format(stock_prices))
                for code in stock_prices:
                    clean_key = 'K_{}'.format(code.replace('.', '_____'))
                    #pd.DataFrame(stock_prices[code]).to_hdf('real_time_prices.h5', key=clean_key, mode='w')
                    hdf[clean_key] = pd.DataFrame(stock_prices[code])
                    #hdf.put(clean_key, pd.DataFrame(stock_prices[code]))
                hdf.close()
                logger_rtapi.info('Data set updated at {}'.format(datetime.now(tz)))

            except ValueError as e:
                logger_rtapi.warning('Error loading data {} '.format(data))


async def live_stock_prices():
    from datetime import datetime
    import datetime as dttime
    import pandas as pd
    from time import sleep
    from pymongo import MongoClient
    from asset_prices.prices import get_prices
    from asset_prices.referencial import get_universe, get_indx_cc_fx_universe
    import pytz
    import requests
    import json

    server_run = '18.191.227.200' # localhost
    #server_run = 'localhost'  # localhost
    collection_name = "real_time_prices"
    db_name = "asset_analytics"
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    tz = pytz.timezone('Europe/Paris')

    #ddf = pd.read_csv("../asset_prices/stock_universe.csv", sep=',', keep_default_na=False)

    ddf = get_universe()
    ddf2 = get_indx_cc_fx_universe()
    ddf = ddf.append(ddf2)
    ddf['full_code'] = ddf['Code'] + '.' + ddf['ExchangeCode']
    lstock = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')


    global real_time_price

    # sio.emit('login', {'userKey': 'streaming_api_key'})
    from aiohttp import ClientSession
    session = ClientSession()
    last_check_now = datetime.now(tz)
    first_run = True

    while True:
        paris_now = datetime.now(tz)
        dtt = paris_now
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0855%z"), '%d%m%Y%H%M%z')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y1930%z"), '%d%m%Y%H%M%z')
        dtt_s = start_date # datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=8, minute=30, tzinfo=tz)
        dtt_e = end_date #datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=18, minute=30, tzinfo=tz)

        logger_rtapi.info('Date check {} < {} < {} '.format(dtt_s, dtt, dtt_e))
        list_to_order = []

        sec = (last_check_now - datetime.now(tz)).seconds
        logger_rtapi.info(
            'Date check {} < {} < {} and {} sec, weekday = {} '.format(dtt_s, dtt, dtt_e, sec, dtt.weekday()))

        if (dtt_s < dtt < dtt_e) and dtt.weekday() <= 4 and (first_run is True or sec >= 500):
            first_run = False
            ddf = get_universe()
            ddf2 = get_indx_cc_fx_universe()
            ddf = ddf.append(ddf2)
            ddf['full_code'] = ddf['Code'] + '.' + ddf['ExchangeCode']
            lstock = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')
            last_check_now = datetime.now(tz)
            sublists = [lstock[x:x + 20] for x in range(0, len(lstock), 20)]
            stringlist = []
            for subset in sublists:
                stringlist.append(','.join(subset))

            for sublist in stringlist:
                list_closing_prices = []

                logger_rtapi.info('Getting data for sub string {}'.format(sublist))
                sreq = "https://eodhistoricaldata.com/api/real-time/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json&s={}"

                async with session.get(sreq.format(sublist)) as response:
                    data = await response.read()
                    #list_closing_prices = await response.json(content_type=None)
                try:
                    list_closing_prices = json.loads(data)
                except ValueError as e:
                    list_closing_prices = []
                    logger_rtapi.warning('Error loading data {} '.format(data))

                if real_time_price is None or dtt_s > dtt > dtt_e:
                    real_time_price = dict()

                if len(list_closing_prices) > 0:
                    # Loading data
                    sreq = "http://{}:5001/api/v1/load_bulk_intraday_stock_prices"
                    str_req = sreq.format(server_run)

                    logger_rtapi.info('Loading {}'.format(str_req))
                    async with session.post(str_req, json=list_closing_prices) as response:
                        data = await response.read()
                    try:
                        stock_prices = json.loads(data)
                        logger_rtapi.info('Seding intraday_prices {}'.format(stock_prices))
                    except ValueError as e:
                        logger_rtapi.warning('Error loading data {} '.format(data))

                    for price in list_closing_prices:
                        if price['timestamp'] != 'NA':
                            await sio.emit('last_traded_price', price)
                            list_to_order.append(price)


sio.event
async def my_event(sid, message):
    await sio.emit('my_response', {'data': message['data']}, room=sid)


@sio.event
async def my_broadcast_event(sid, message):
    await sio.emit('my_response', {'data': message['data']})


@sio.event
async def join(sid, message):
    sio.enter_room(sid, message['room'])
    await sio.emit('my_response', {'data': 'Entered room: ' + message['room']},
                   room=sid)


@sio.event
async def leave(sid, message):
    sio.leave_room(sid, message['room'])
    await sio.emit('my_response', {'data': 'Left room: ' + message['room']},
                   room=sid)


@sio.event
async def close_room(sid, message):
    await sio.emit('my_response',
                   {'data': 'Room ' + message['room'] + ' is closing.'},
                   room=message['room'])
    await sio.close_room(message['room'])


@sio.event
async def my_room_event(sid, message):
    await sio.emit('my_response', {'data': message['data']},
                   room=message['room'])


@sio.event
async def disconnect_request(sid):
    await sio.disconnect(sid)


@sio.event
async def connect(sid, environ):
    await sio.emit('my_response', {'data': 'Connected', 'count': 0}, room=sid)


@sio.event
def disconnect(sid):
    logger_rtapi.info('Client disconnected')


#app.router.add_static('/static', 'static')
#app.router.add_get('/', index)
#app.static('/static', './static')


if __name__ == '__main__':
    #sio.start_background_task(live_stock_prices)
    app.run(port=5000, host='0.0.0.0')