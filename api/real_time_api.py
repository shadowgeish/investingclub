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
    return True


def is_serializable(obj):
    import json
    try:
        json.dumps(obj)
    except TypeError:
        logger_rtapi.error("Unable to serialize the object")
        return False

    return True


async def live_stock_prices():
    from datetime import datetime
    import pandas as pd
    from time import sleep
    from pymongo import MongoClient
    from asset_prices.prices import get_prices
    from asset_prices.referencial import get_universe
    import pytz

    tz = pytz.timezone('Europe/Paris')
    paris_now = datetime.now(tz)

    collection_name = "real_time_prices"
    db_name = "asset_analytics"
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    dtt = paris_now
    dtt_s = datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=00, minute = 0, tzinfo=tz)
    dtt_e = datetime(year=dtt.year, month=dtt.month, day=dtt.day, hour=23, minute=00, tzinfo=tz)

    logger_rtapi.info('Date check {} < {} < {} '.format(dtt_s, dtt, dtt_e))
    #ddf = pd.read_csv("../asset_prices/stock_universe.csv", sep=',', keep_default_na=False)

    ddf = get_universe()

    ddf['full_code'] = ddf['Code'] + '.' + ddf['ExchangeCode']
    lstock = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')

    lstock = lstock[0:20]
    sublists = [lstock[x:x + 20] for x in range(0, len(lstock), 20)]
    stringlist = []
    global real_time_price
    for subset in sublists:
        stringlist.append(','.join(subset))

    # sio.emit('login', {'userKey': 'streaming_api_key'})
    from aiohttp import ClientSession
    session = ClientSession()
    while True:

        if dtt_s < dtt < dtt_e:

            logger_rtapi.info('Date check {} < {} < {} '.format(dtt_s, dtt, dtt_e))
             
            import pandas as pd
            import requests
            import json
            list_to_order = []
            for sublist in stringlist:
                logger_rtapi.info('Getting data for sub string {}'.format(sublist))
                sreq = "https://eodhistoricaldata.com/api/real-time/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json&s={}"
                list_closing_prices = []

                async with session.get(sreq.format(sublist)) as response:
                    list_closing_prices = await response.json()


                #list_closing_prices = req.json()
                # print('Reveived data {}, {}'.format(sublist, list_closing_prices))
                # Real time prices

                if real_time_price is None:
                    real_time_price = dict()

                for price in list_closing_prices:
                    code = price['code']
                    key = price['code']  # + '_' + request_day
                    # push the price data to the socket so they could quicky see it
                    # print('sending data for {}'.format(price))
                    # print('Data check  {} : is {}'.format(is_serializable(price) ,price))
                    await sio.emit('last_traded_price', price)
                    list_to_order.append(price)


                    # push to the data price base for that code
                    # DATA BASE
                    if key not in real_time_price.keys():
                        real_time_price[key] = list()
                        # read from db if there is historic for the day to real_time_price
                        # READ DATA BASE
                        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
                        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')
                        lk = [key]
                        logger_rtapi.info('start_date = {}, end_date = {}'.format(start_date, end_date))
                        rt_price_df = get_prices(asset_codes=lk, start_date=start_date, end_date=end_date,
                                                 type='real_time', ret='df')
                        real_time_price[key] = rt_price_df.to_dict(orient='records')
                        logger_rtapi.info('Already loaded data {}'.format(rt_price_df))

                    current_list = real_time_price[key]
                    exists = 0
                    for item in current_list:
                        if item['timestamp'] == price['timestamp']:
                            exists = 1
                            break
                    if exists == 0 and price['timestamp'] != 'NA':
                        # Add each item of the list if doesn't exist
                        price['converted_date'] = price['timestamp']

                        logger_rtapi.info('Price {}'.format(price))

                        price['date'] = datetime.fromtimestamp(price['timestamp']).strftime("%d-%m-%Y %H:%M:%S.%f")

                        server[db_name][collection_name].update_one({"code": key}, {"$addToSet": {
                            "prices": price}}, upsert=True)
                        real_time_price[key].append(price)

                    # push the json array of the day to the socket
                    list_data = {'code': key, 'prices': real_time_price[key]}
                    # print('Data chech  {} : is {}'.format(is_serializable(list_data), list_data))
                    await sio.emit('intraday_prices', list_data)


            dic_to_order = pd.DataFrame(list_to_order)
            dic_to_order = dic_to_order[['code','volume','timestamp']]
            dic_to_order = dic_to_order[dic_to_order['timestamp'] != 'NA']
            logger_rtapi.info('dic_to_order {}'.format(dic_to_order))

            dic_to_order = dic_to_order.sort_values(by=['volume'], ascending=False).reset_index()
            dic_to_order['order'] = dic_to_order.index
            dic_to_order = dic_to_order[['code', 'volume', 'timestamp', 'order']]
            await sio.emit('intraday_trending_stocks', dic_to_order.to_json(orient='records'))
            logger_rtapi.info('col ={}, Dicts = {}'.format(dic_to_order.columns, dic_to_order))

            await sio.sleep(10)
        
        else:
            real_time_price = dict()


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