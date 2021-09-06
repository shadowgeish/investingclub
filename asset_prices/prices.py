#!/usr/bin/env python
"""

Prices module

"""
from datetime import date


# mongodb+srv://sngoube:<password>@cluster0.jaxrk.mongodb.net/<dbname>?retryWrites=true&w=majority
# mongodb+srv://sngoube:Came1roun*@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority
# pip install dnspython==2.0.0

# requirements
# dnspython
# Cython==0.29.17
# setuptools==49.6.0
# statsmodels==0.11.1
# fbprophet
# pandas
# numpy
# matplotlib
# plotly
# pymongo
# pymongo[srv]
# alpha_vantage
# requests
# u8darts
import pandas as pd
import requests
from pymongo import MongoClient
import json
import datetime
import os
import re
from tools.logger import logger_get_price


def load_stock_historical_data_mp(args):
    (stock, start_date,end_date, eod_key, full_available_history) = args
    load_stock_historical_data(stock, start_date,end_date, eod_key, full_available_history)


def load_stock_historical_data(stock=None, start_date =None,end_date = None, eod_key = None,
                               full_available_history=False):
    logger_get_price.info('stock = {}, start_date{} end_date= {} eod_key = {} '.format(stock, start_date, end_date,
                                                                                       eod_key))
    # https://eodhistoricaldata.com/api/eod/{}?from={} & to = {} & api_token = {} & period = d & fmt = json
    req = requests.get("https://eodhistoricaldata.com/api/eod/{}?from={}&to={}&api_token={}&period=d&fmt=json".format(
        stock,
        start_date,
        end_date,
        eod_key))

    collection_name = "historical_prices"
    db_name = "asset_analytics"
    # https://eodhistoricaldata.com/api/eod/BX4.PA?from=2020-01-05&to=2020-02-10&api_token=60241295a5b4c3.00921778&period=d
    #
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    collection_handler = server[db_name][collection_name]
    try:
        list_closing_prices = req.json()
    except:
        logger_get_price.warn('Error: stock = {}, start_date = {} end_date= {} eod_key = {} , req = {}  '.format(stock, start_date, end_date,
                                                                                           eod_key, req))
        return


    if isinstance(list_closing_prices, dict):
        list_closing_prices = [] if 'errors' in list_closing_prices else [list_closing_prices]

    if len(list_closing_prices) == 0:
        logger_get_price.info('No data for stock {}'.format(stock))
        return

    ddf = pd.DataFrame().from_records(list_closing_prices)
    ddf['code'] = stock

    ddf['converted_date'] = ddf['date'].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").timestamp())

    # ddf['converted_date'] = pd.to_datetime(ddf['date'], format="%Y-%m-%d") # datetime.datetime.strptime("2017-10-13T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.000Z")

    list_closing_prices = json.loads(ddf.to_json(orient='records'))

    stock_data = {"code": stock,
                  "prices": list_closing_prices
                  }

    if full_available_history is True:
        collection_handler.delete_one({"code": stock})
        collection_handler.insert_one(stock_data)
        logger_get_price.info("full load for {} completed!".format(stock))
    else:
        # dd = server[db_name][collection_name].update_many({"code": stock}, {"$addToSet": {
        #    "prices": {"volume": 10000000, "date": "2021-02-14", 'open': 1, 'close': 2, 'low': 5, 'high': 4,
        #               'adjusted_close': 6}}})

        # Add each item of the list if doesn't exist
        collection_handler.update_many({"code": stock}, {"$addToSet": {
            "prices": {"$each": list_closing_prices}}})

        logger_get_price.info("update load for {} completed!".format(stock))


def load_historical_data(asset_ticker=None, full_available_history=False,
                        ret='json' # json, df
                         ):
    #  db mp jCJpZ8tG7Ms3iF0l
    eod_key = "60241295a5b4c3.00921778"
    # https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json
    start_date = "2000-01-05"
    e_date = datetime.datetime.today() + datetime.timedelta(1)
    end_date = e_date.strftime("%Y-%m-%d")
    if full_available_history is True: # Load 20 years history
        start_date = "2005-01-05"
    else:
        s_date = datetime.datetime.today() + datetime.timedelta(-2)
        start_date = s_date.strftime("%Y-%m-%d")

    collection_name = "historical_prices"
    db_name = "asset_analytics"
    # https://eodhistoricaldata.com/api/eod/BX4.PA?from=2020-01-05&to=2020-02-10&api_token=60241295a5b4c3.00921778&period=d
    #
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    #list_stocks = [asset_ticker] if asset_ticker is not None else list(server[db_name]["stocks"].find({}))

    from referencial import get_universe, get_indx_cc_fx_universe
    if asset_ticker is None:
        ddf = get_universe()
        ddf2 = get_indx_cc_fx_universe()
        ddf = ddf.append(ddf2)
        ddf['full_code'] = ddf['Code'] + '.' + ddf['ExchangeCode']
        list_stocks = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')
    else:
        list_stocks = asset_ticker
    #ist_stocks = list_stocks[0:5]
    i = 1
    collection_handler = server[db_name][collection_name]

    import multiprocessing as mp
    with mp.Pool(processes=mp.cpu_count()) as p:
        logger_get_price.info("--- start simul for {} stocks --- and  using {} cpu".format(len(list_stocks), mp.cpu_count()))
        list_var_tuple = [(stock, start_date, end_date, eod_key, full_available_history) for stock in list_stocks]
        results = p.map(load_stock_historical_data_mp, list_var_tuple)

    ''' 
    for stock in list_stocks:

        print('{}/{} - {} : from {} to {} '.format(i,len(list_stocks), stock, start_date, end_date))
        #https://eodhistoricaldata.com/api/eod/{}?from={} & to = {} & api_token = {} & period = d & fmt = json
        req = requests.get("https://eodhistoricaldata.com/api/eod/{}?from={}&to={}&api_token={}&period=d&fmt=json".format(
            stock,
            start_date,
            end_date,
            eod_key))

        list_closing_prices = req.json()
        if isinstance(list_closing_prices, dict):
            list_closing_prices = [] if 'errors' in list_closing_prices else [list_closing_prices]

        if len(list_closing_prices) == 0:
            print('No data for stock {} - {}'.format(i, stock))
            i = i + 1
            continue

        ddf = pd.DataFrame().from_records(list_closing_prices)
        ddf['code'] = stock

        ddf['converted_date'] = ddf['date'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").timestamp())

        # ddf['converted_date'] = pd.to_datetime(ddf['date'], format="%Y-%m-%d") # datetime.datetime.strptime("2017-10-13T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.000Z")

        list_closing_prices = json.loads(ddf.to_json(orient='records'))

        stock_data = {"code": stock,
                      "prices": list_closing_prices
                      }

        if full_available_history is True:
            server[db_name][collection_name].delete_one({"code": stock})
            server[db_name][collection_name].insert_one(stock_data)
            logger_get_price.info("full load for {} completed!".format(stock))
        else:
            # dd = server[db_name][collection_name].update_many({"code": stock}, {"$addToSet": {
            #    "prices": {"volume": 10000000, "date": "2021-02-14", 'open': 1, 'close': 2, 'low': 5, 'high': 4,
            #               'adjusted_close': 6}}})

            # Add each item of the list if doesn't exist
            server[db_name][collection_name].update_many({"code": stock}, {"$addToSet": {
                "prices": {"$each": list_closing_prices}}})

            logger_get_price.info("update load for {} completed!".format(stock))
        i = i + 1
    '''

    logger_get_price.info("Done!")
    server.close()


def update_prices(asset_code=None, price=None, type='real_time'):
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import datetime
    import pytz

    collection_name = "real_time_prices" if type == 'real_time' else 'historical_prices'
    db_name = "asset_analytics"

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    server[db_name][collection_name].update_one({"code": asset_code}, {"$addToSet": {
        "prices": price}}, upsert=True)

# return historical/real time data for one or a list of codes example code : "BX4.PA"
# usage get_historical_data(asset_codes="BX4.PA")  returns 1 week history for code BX4.PA
# usage get_historical_data(asset_codes=["BX4.PA", "CAC.PA"])  returns 1 week history for code BX4.PA and CAC.PA
def get_prices(asset_codes=[],
                        start_date=None,
                        end_date=None,
                        type='historical',
                        ret_code=0, # json, df
                        ret='json' # json, df
                        ):
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import datetime
    import pytz

    tz = pytz.timezone('Europe/Paris')
    paris_now = datetime.datetime.now(tz)

    logger_get_price.info("dates sent from {} to {}".format(start_date, end_date))

    eod_key = "60241295a5b4c3.00921778"
    sd = datetime.datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + datetime.timedelta(-7)
    ed = datetime.datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + datetime.timedelta(+1)
    start_date = sd if start_date is None else start_date
    end_date = ed if end_date is None else end_date

    logger_get_price.info("dates Computed from {} to {}".format(start_date, end_date))

    collection_name = "real_time_prices" if type == 'real_time' else 'historical_prices'
    db_name = "asset_analytics"

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    list_stocks = asset_codes if isinstance(asset_codes, list) else [asset_codes]
    sdate = start_date.timestamp()
    edate = end_date.timestamp()
    if ret_code == 1:
        query = [
            {"$match": {"code": {"$in": list_stocks}}},
            {"$project": {"prices":
                {
                    "$filter": {
                        "input": "$prices",
                        "as": "price",
                        "cond": {
                            "$and": [
                                {"$gte": ["$$price.converted_date", sdate]},
                                {"$lte": ["$$price.converted_date", edate]}
                            ]
                        }
                    }
                },
                "code": ret_code
            }
            }]
    else:
        query = [
            {"$match": {"code": {"$in": list_stocks}}},
            {"$project": {"prices":
                {
                    "$filter": {
                        "input": "$prices",
                        "as": "price",
                        "cond": {
                            "$and": [
                                {"$gte": ["$$price.converted_date", sdate]},
                                {"$lte": ["$$price.converted_date", edate]}
                            ]
                        }
                    }
                }
            }
            }]
    res = server[db_name][collection_name].aggregate(query)
    #1612738806575.399
    #1613088000000
    lres = list(res)
    item_list = []
    for doc in lres:# loop through the documents
        item_list = item_list + doc['prices']
    df = pd.DataFrame(item_list)

    #   logger_get_price.info(format(df.to_json(orient='records')))

    logger_get_price.info("Done get_prices: {}, query : {}: dates from {} to {}".format(type, query, start_date,
                                                                                        end_date))
    server.close()

    if ret == 'json':
        return df.to_json(orient='records')
    else:
        return df

    """ 
    server[db_name][collection_name].aggregate([
        {"$match": {"code": {"$in": ["500.PA", "BX4.PA"] } }},
        {"$project": {"prices":
            {
                "$filter": {
                    "input": "$prices",
                    "as": "price",
                    "cond": {
                        "$and": [
                            {"$gte": ["$$price.converted_date", 1613088000000]}
                        ]
                    }
                }
            }
        }
        }])
    """


if __name__ == '__main__':
    load_historical_data(full_available_history=True)
    #logger_get_price.info(get_prices(asset_codes=["IWDA.LSE", "TDT.AS", "BX4.PA", "LVC.PA"], ret = 'df'))

