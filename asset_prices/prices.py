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
from api.utils import *

def load_stock_historical_data_mp(args):
    (stock, start_date,end_date, eod_key, full_available_history) = args
    load_stock_historical_data(stock, start_date,end_date, eod_key, full_available_history)


def load_stock_historical_data(stock=None, start_date =None, end_date = None, eod_key = None,
                               full_available_history=False):
    logger_get_price.info('stock = {}, start_date{} end_date= {} eod_key = {} '.format(stock, start_date, end_date,
                                                                                       eod_key))
    # https://eodhistoricaldata.com/api/eod/{}?from={} & to = {} & api_token = {} & period = d & fmt = json
    req_url = "https://eodhistoricaldata.com/api/eod/{}?from={}&to={}&api_token={}&period=d&fmt=json".format(
        stock,
        start_date,
        end_date,
        eod_key)
    req = requests.get(req_url)

    logger_get_price.info(
        'Req = {} , stock = {}, start_date = {} end_date= {} eod_key = {}  '.format(req_url, stock, start_date, end_date,
                                                                                           eod_key ))

    collection_name = "historical_prices"
    db_name = "asset_analytics"
    collection_name_stock = "stock_data"
    # https://eodhistoricaldata.com/api/eod/BX4.PA?from=2020-01-05&to=2020-02-10&api_token=60241295a5b4c3.00921778&period=d
    #
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    collection_handler = server[db_name][collection_name_stock]
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

    stock_data = {"FullCode": stock,
                  "prices": list_closing_prices
                  }

    collection_handler.update_many({"FullCode": stock}, {"$addToSet": {
            "prices": {"$each": list_closing_prices}}})

    collection_handler.update({"FullCode": stock}, {"$set": {
        "last_price": list_closing_prices[-1]['adjusted_close']} })

    collection_handler.update({"FullCode": stock}, {"$set": {
        "last_price_date": list_closing_prices[-1]['date']}})

    collection_handler.update({"FullCode": stock}, {"$set": {
        "last_price_volume": list_closing_prices[-1]['volume']}})

    #if full_available_history is True:
    #    collection_handler.delete_one({"code": stock})
    #    collection_handler.insert_one(stock_data)
    #    logger_get_price.info("full load for {} completed!".format(stock))
    #else:
        # Add each item of the list if doesn't exist
    #    collection_handler.update_many({"code": stock}, {"$addToSet": {
    #        "prices": {"$each": list_closing_prices}}})
    #    logger_get_price.info("update load for {} completed!".format(stock))


def load_historical_data(asset_ticker=None, full_available_history=True,
                        ret='json', # json, df
                         enrich_stock_data = 0
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

    logger_get_price.info("Done!")
    server.close()


def compute_portfolio_analytics(params={}):
    from asset_prices.referencial import get_universe

    logger_get_price.info("params ={}".format(params))
    portfolio_id = params['portfolioId']
    benchmark_stock_code = params['benchmarkStockCode']
    portfolio_historical_holdings = params['portfolioHistoricalHoldings']
    min_date, max_date = min_max_dates(portfolio_historical_holdings)
    stock_code_list = stock_list(portfolio_historical_holdings)
    stock_data_df = get_universe(codes=",".join(stock_code_list))
    stock_data_df['code'] = stock_data_df['Code'] + '.' + stock_data_df['ExchangeCode']
    logger_get_price.info("stock_data_df ={}".format(stock_data_df))
    stock_data_list = stock_data_df.to_dict('records')
    full_stock_code_list = stock_code_list
    full_stock_code_list.append(benchmark_stock_code)

    df_h_p = get_prices(asset_codes=full_stock_code_list, ret='df',
                        start_date=datetime.datetime.combine(min_date, datetime.time.min) + datetime.timedelta(days=-1),
                        end_date=datetime.datetime.combine(max_date, datetime.time.min)+ datetime.timedelta(days=1),
                        )

    logger_get_price.info("df_h_p ={}".format(df_h_p))

    df_bhp = df_h_p[df_h_p['code'] == benchmark_stock_code].copy()

    stock_prices_list = df_h_p[df_h_p['code'] != benchmark_stock_code].copy().to_dict('records')

    #logger_get_price.info("df_bhp ={}".format(df_bhp))

    df_h_sp = df_h_p[df_h_p['code'] != benchmark_stock_code].copy()

    portfolio_historical_values, portfolio_stock_values, portfolio_stock_weights, last_portfolio_stock_values = stock_total_value(portfolio_historical_holdings, df_h_sp, stock_data_df, stock_code_list)


    #asset_type_values = {}
    #if 'holdings' in last_portfolio_stock_values.keys():
    asset_type_values = get_asset_type_grouping(stock_data_list, {'values': last_portfolio_stock_values['holdings']}, group_by='values')

    dfvals = pd.DataFrame(portfolio_historical_values)
    dfvals.index = pd.to_datetime(dfvals['date'])
    dfvals = dfvals.drop(columns=['date'])

    dfvals = dfvals.sort_index()
    print(" dfvals = {}".format(dfvals))
    daily_returns, monthly_returns = get_returns(dfvals)

    #  benchmark
    df_bhp = df_bhp[['date', 'close']]
    df_bhp.index = pd.to_datetime(df_bhp['date'])
    df_bhp = df_bhp.drop(columns=['date'])
    b_daily_returns, b_monthly_returns = get_returns(df_bhp)
    logger_get_price.info("b_daily_returns ={}".format(b_daily_returns))

    rt_daily_returns, rt_monthly_returns = daily_returns.copy(), monthly_returns.copy()
    rt_daily_returns['date'] = rt_daily_returns.index
    rt_daily_returns['date'] = rt_daily_returns['date'].dt.strftime('%Y-%m-%d')
    rt_monthly_returns['date'] = rt_monthly_returns.index
    rt_monthly_returns['date'] = rt_monthly_returns['date'].dt.strftime('%Y-%m-%d')

    asset_type_weights = get_asset_type_grouping_weights(asset_type_values, last_portfolio_stock_values['portfolio_value'])

    port_volatility = volatility(daily_returns).to_numpy()[-1]

    g_daily_returns, g_b_daily_returns = daily_returns.dropna(), b_daily_returns.dropna()
    logger_get_price.info("g_daily_returns ={}, g_b_daily_returns ={}".format(g_daily_returns, g_b_daily_returns))
    beta, alpha = greeks(g_daily_returns, g_b_daily_returns)

    logger_get_price.info("result beta {}, alpha {}".format(beta, alpha))

    sharpe_ratio = sharpe(g_daily_returns).to_numpy()[-1]

    drawdown = max_drawdown(dfvals).to_numpy()[-1]

    best_perf = g_daily_returns.max().to_numpy()[-1]
    worst_perf = g_daily_returns.min().to_numpy()[-1]
    number_up = int(dfvals.pct_change().fillna(0).gt(0).sum().to_numpy()[-1])
    number_down = int(dfvals.pct_change().fillna(0).lt(0).sum().to_numpy()[-1])

    last_date = last_holding_date(portfolio_stock_values, max_date.strftime("%Y-%m-%d"),
                                  min_date.strftime("%Y-%m-%d"))

    winners, loosers = winners_loosers(stock_prices_list, max_date.strftime("%Y-%m-%d"), last_date)



    analytics = { "portfolio_id":portfolio_id,
                  "min_date": min_date.strftime("%Y-%m-%d"),
                  "max_date": max_date.strftime("%Y-%m-%d"),
                  "stocks": ",".join(stock_code_list),
                  "portfolio_stock_values":portfolio_stock_values,
                  "portfolio_historical_values" : portfolio_historical_values,
                  "last_portfolio_stock_values" : last_portfolio_stock_values,
                  "portfolio_stock_weights":portfolio_stock_weights,
                  "asset_type_values":asset_type_values,
                  "daily_returns":rt_daily_returns.to_dict('records'),
                  "monthly_returns":rt_monthly_returns.to_dict('records')
                  ,"asset_type_weights":asset_type_weights
                  ,"port_volatility": port_volatility
                  ,"beta":beta
                  ,"alpha": alpha
                  ,"sharpe_ratio":sharpe_ratio
                  ,"drawdown": drawdown
                  ,"best_perf": best_perf
                  ,"worst_perf" : worst_perf
                  ,"number_up":number_up
                  ,"number_down": number_down
                  ,"winners":winners
                  ,"loosers":loosers
                  }

    return analytics
'''
{
    "FullCode":"BTC-USD.CC",
    "General":
     {
       "Name":"Bitcoin",
       "Exchange":"CC",
       "Code":"BTC-USD",
       "Description":"Bitcoin value in Dollars"
     },
    "Code":"BTC-USD",
    "Country":"US",
    "Exchange":"CC",
    "Type":"CryptoCurrency",
    "Active":1,
    "AssetType":"CryptoCurrency"
}
 '''


def update_bulk_prices(prices=None, type='real_time'):
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    from datetime import datetime
    import pytz

    collection_name = "real_time_prices" if type == 'real_time' else 'historical_prices'
    stock_data = "stock_data"
    db_name = "asset_analytics"

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)
    tz = pytz.timezone('Europe/Paris')

    if len(prices) == 0:
        return

    paris_now = datetime.now(tz)
    start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700%z"), '%d%m%Y%H%M%z')
    end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300%z"), '%d%m%Y%H%M%z')
    list_codes = [ price['code'] for price in prices ]
    rt_price_df = get_prices(asset_codes=list_codes, start_date=start_date, end_date=end_date,
                             type='real_time', ret_code=1, ret='df')

    for price in prices:
        if price['timestamp'] != 'NA':
            asset_code = price['code']
            #rt_price_df = get_prices(asset_codes=[asset_code], start_date=start_date, end_date=end_date,
            #                         type='real_time', ret_code=0, ret='df')
            #current_prices = rt_price_df.to_dict(orient='records')
            current_prices = []
            if 'code' in rt_price_df.columns:
                current_price_df = rt_price_df[ rt_price_df['code']==asset_code]
                current_prices = current_price_df.to_dict(orient='records')
                print('!!!!!!current_prices = {}'.format(current_prices))

            server[db_name][collection_name].update_one({"code": asset_code}, {"$set": {
                "prices": current_prices}})

            price['converted_date'] = price['timestamp']
            price['date'] = datetime.fromtimestamp(price['timestamp'], tz = tz).strftime("%d-%m-%Y %H:%M")
            server[db_name][collection_name].update_one({"code": asset_code}, {"$addToSet": {
                "prices": price}}, upsert=True)

            #server[db_name][collection_name].update_one({"code": asset_code}, {"$set": {
            #    "prices": price}}, upsert=True)

            server[db_name][stock_data].update({"FullCode": price['code']}, {"$set": {
                "last_price_rt": price['close']}})

            server[db_name][stock_data].update({"FullCode": price['code']}, {"$set": {
                "last_price_date_rt": price['date']}})

            server[db_name][stock_data].update({"FullCode": price['code']}, {"$set": {
                "last_price_change_p_rt": price['change_p']}})

            server[db_name][stock_data].update({"FullCode": price['code']}, {"$set": {
                "last_price_volume_rt": price['volume']}})

    logger_get_price.info("prices loaded {}".format(prices))

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

    logger_get_price.info("price loaded {}".format(price))

    server.close()

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
    sd = datetime.datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + datetime.timedelta(-200)
    ed = datetime.datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + datetime.timedelta(+1)
    start_date = sd if start_date is None else start_date
    end_date = ed if end_date is None else end_date

    logger_get_price.info("dates Computed from {} to {}".format(start_date, end_date))
    if type == 'real_time':
        collection_name = "real_time_prices"
        code = 'code'
    else:
        collection_name = 'stock_data' #'historical_prices'
        code = 'FullCode'
    db_name = "asset_analytics"

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    list_stocks = asset_codes if isinstance(asset_codes, list) else [asset_codes]
    sdate = start_date.timestamp()
    edate = end_date.timestamp()
    if ret_code == 1:
        query = [
            {"$match": {code: {"$in": list_stocks}}},
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
            {"$match": {code: {"$in": list_stocks}}},
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

    logger_get_price.info("Data returned {}".format(df))
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

