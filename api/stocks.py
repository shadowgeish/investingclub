from collections import OrderedDict

from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from datetime import datetime
from analytics.analytics import monte_carlo_portfolio_simul
from asset_prices.referencial import get_universe

stock_universe_request_parser = RequestParser(bundle_errors=False)

stock_universe_request_parser.add_argument("name", type=str, required=False,
                                        help="Stock name", default="")

stock_universe_request_parser.add_argument("country", type=str, required=False,
                                        help="Country", default="")

stock_universe_request_parser.add_argument("type", type=int, required=False,
                                        help="type", default="")

stock_universe_request_parser.add_argument("sector", type=int, required=False,
                                        help="sector", default="")

stock_universe_request_parser.add_argument("cache", type=str, required=False,
                                        help="cache", default="false")

stock_universe_request_parser.add_argument("skip", type=int, required=False,
                                        help="skip", default=1)

stock_universe_request_parser.add_argument("limit", type=int, required=False,
                                        help="limit", default=10)


class StockUniverse(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self):
        import datetime
        import pandas as pd
        import requests
        from pymongo import MongoClient
        import json
        import datetime

        args = stock_universe_request_parser.parse_args()

        name = args['name']
        country = args['country']
        type = args['type']
        sector = args['sector']
        skip = args['skip']
        limit = args['limit']

        df = get_universe(name=name, country=country, type=type, sector=sector, skip=skip, limit=limit)

        # result = df.to_json(orient='records')
        result = df.to_dict(orient='records')
        #print('json result {}'.format(result))

        return result, 200

        ''' 
        collection_name = "stock_universe"
        db_name = "asset_analytics"
        access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
        server = MongoClient(access_db)

        query = {"Name": {"$regex": '/*{}/*'.format(name), "$options": 'i'},
                 "Country": {"$regex": '/*{}/*'.format(country), "$options": 'i'},
                 "Type": {"$regex": '/*{}/*'.format(type), "$options": 'i'}
                 }
        res = server[db_name][collection_name].find(query)

        print("Query {}".format(query))

        lres = list(res)

        item_list = []
        result = []
        if len(lres) > 0:
            print(' result {}'.format(lres))
            df = pd.DataFrame(lres)
            print(' df {}'.format(df))
            df = df[['ISIN', 'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'ExchangeCode', 'logo']]
            # print(format(df.to_json(orient='records')))
            server.close()
            print(' result {}'.format(df))
            # result = df.to_json(orient='records')
            result = df.to_dict(orient='records')
            print('json result {}'.format(result))

        return result, 200
        '''


class StockUniverseIntradayData(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, codes):
        import pandas as pd
        from datetime import datetime
        import pytz
        args = stock_universe_request_parser.parse_args()
        name = args['name']
        country = args['country']
        type = args['type']
        sector = args['sector']
        cache = args['cache']
        skip = args['skip']
        limit = args['limit']

        tz = pytz.timezone('Europe/Paris')

        if codes == 'All':
            df = get_universe(name=name, country=country, type=type, sector=sector, skip=skip, limit=limit)
            df['full_code'] = df['Code'] + '.' + df['ExchangeCode']
            lstock = df['full_code'].tolist()
        else:
            lstock = codes.split(',')

        ''' 
        if cache == "true":
            print('READ FROM THE CACHE!!')
            s_now = datetime.now(tz)
            map_dt = {}
            # pd.DataFrame(stock_prices[code]).to_hdf('real_time_prices.h5', key=clean_key, mode='w')
            # result = rt_price_df.to_dict(orient='records')
            # pd.read_hdf('data.h5', 'df')
            hdf = pd.HDFStore('real_time_prices.h5', 'r')
            #print('Available Codes {}'.format(hdf.keys()))
            for key in hdf.keys():
                clean_key = key[3:].replace('_____', '.')
                if clean_key in lstock:
                    map_dt[clean_key] = pd.read_hdf('real_time_prices.h5', key).to_dict(orient='records')
            hdf.close()
            print('READ FROM THE CACHE in {}!!'.format((s_now - datetime.now(tz)).total_seconds()))
            return map_dt, 200
        
        '''

        print('READ FROM THE DATABASE!!')
        s_now = datetime.now(tz)



        from asset_prices.prices import get_prices


        paris_now = datetime.now(tz)
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')

        rt_price_df = get_prices(asset_codes=lstock, start_date=start_date, end_date=end_date,
                                 type='real_time', ret_code=1, ret='df')

        if 'volume' in rt_price_df.columns:
            rt_price_df = rt_price_df.sort_values(by=['volume'], ascending = False )

        result = rt_price_df.to_dict(orient='records')

        dict_prices = OrderedDict()
        for price in result:
            if price['code'] not in dict_prices.keys():
                dict_prices[price['code']] = list()
            dict_prices[price['code']].append(price)

        print('READ FROM THE DATABASE in {}!!'.format((s_now - datetime.now(tz)).total_seconds()))

        return dict_prices, 200


class StockData(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, code):
        import datetime
        import pandas as pd
        import requests
        from bson import json_util
        from pymongo import MongoClient
        import json
        import datetime

        # code = 'FR.PA'
        collection_name = "stock_data"
        db_name = "asset_analytics"
        access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
        server = MongoClient(access_db)

        query = {"FullCode": code}
        res = server[db_name][collection_name].find_one(query, {'_id': 0 ,
                                                                 'ETF_Data.Market_Capitalisation': 0,
                                                                 'ETF_Data.Asset_Allocation': 0,
                                                                 'ETF_Data.Valuations_Growth': 0
                                                                 })

        res2 = server[db_name]["stock_esg_data"].find_one(query, {'_id': 0})

        json_res = {} if res is None else json.loads(json_util.dumps(res))
        json_res2 = {} if res2 is None else json.loads(json_util.dumps(res2))

        merged_dict = dict(json_res, **json_res2)

        print("Query {} and result {}".format(query, merged_dict))

        return merged_dict, 200


intraday_stock_prices_request_parser = RequestParser(bundle_errors=False)

intraday_stock_prices_request_parser.add_argument("timestamp", type=int, required=False,
                                        help="timestamp", default=0)

intraday_stock_prices_request_parser.add_argument("gmtoffset", type=int, required=False,
                                        help="gmtoffset", default=0)

intraday_stock_prices_request_parser.add_argument("open", type=float, required=False,
                                        help="open", default=0)

intraday_stock_prices_request_parser.add_argument("high", type=float, required=False,
                                        help="high", default=0)

intraday_stock_prices_request_parser.add_argument("low", type=float, required=False,
                                        help="low", default=0)

intraday_stock_prices_request_parser.add_argument("close", type=float, required=False,
                                        help="close", default=0)

intraday_stock_prices_request_parser.add_argument("volume", type=int, required=False,
                                        help="volume", default=0)

intraday_stock_prices_request_parser.add_argument("previousClose", type=float, required=False,
                                        help="previousClose", default=0)

intraday_stock_prices_request_parser.add_argument("change", type=float, required=False,
                                        help="change", default=0)

intraday_stock_prices_request_parser.add_argument("change_p", type=float, required=False,
                                        help="change_p", default=0)

intraday_stock_prices_request_parser.add_argument("converted_date", type=int, required=False,
                                        help="converted_date", default=0)

intraday_stock_prices_request_parser.add_argument("date", type=str, required=False,
                                        help="date", default="")


class IntradayStockPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, codes):

        from asset_prices.prices import get_prices
        from datetime import datetime
        import pytz

        tz = pytz.timezone('Europe/Paris')
        paris_now = datetime.now(tz)
        last_check_now = datetime.now(tz)
        dtt = paris_now
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')
        codes_list = codes.split(',')
        rt_price_df = get_prices(asset_codes=codes_list, start_date=start_date, end_date=end_date,
                                type='real_time',ret_code=1, ret='df')

        result = rt_price_df.to_dict(orient='records')
        print('Real time data {}'.format(rt_price_df))

        print('json result {}'.format(result))

        return result, 200


class PushBulkIntradayStockPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def post(self):

        from asset_prices.prices import get_prices, update_bulk_prices
        from datetime import datetime
        import pytz
        from flask import Flask, request, jsonify

        tz = pytz.timezone('Europe/Paris')
        paris_now = datetime.now(tz)
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')

        # Get POST data as json & read it as a DataFrame
        prices = request.get_json()

        print('Price {}'.format(prices))

        update_bulk_prices(prices=prices, type='real_time')

        result = {'Load':'OK'}

        print('json result {}'.format(result))

        return result, 200


class PushIntradayStockPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, code):

        from asset_prices.prices import get_prices, update_prices
        from datetime import datetime
        import pytz


        tz = pytz.timezone('Europe/Paris')
        paris_now = datetime.now(tz)
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')

        #CAC.PA?timestamp = 1630932780 & gmtoffset = 0 & open = 65.94 & high = 66.33 & low = 65.94 & close = 66.33 & volume = 11065 & previousClose = 65.75 & change = 0.58 & change_p = 0.8821 & converted_date = 1630932780 & date = 06 - 0
        #9 - 2021 % 2014: 53:00.000000

        try:
            args = intraday_stock_prices_request_parser.parse_args()
        except:
            import sys
            return {'error': '{}'.format(sys.exc_info()[0])}, 200
            print("Oops!", sys.exc_info()[0], "occurred.")

        price ={
            "code": code,
            "timestamp": args["timestamp"],
            "gmtoffset": args["gmtoffset"],
            "open": args["open"],
            "high": args["high"],
            "low": args["low"],
            "close": args["close"],
            "volume": args["volume"],
            "previousClose": args["previousClose"],
            "change": args["change"],
            "change_p": args["change_p"],
            "converted_date": args["converted_date"],
            "date": args["date"]
        }
        print('Price {}'.format(price))

        update_prices(asset_code=code, price=price, type='real_time')

        rt_price_df = get_prices(asset_codes=[code], start_date=start_date, end_date=end_date,
                                type='real_time', ret='df')

        import os
        #if not os.path.isfile('./swmr_real_time_prices.csv'):
        #    f["run_date"] = paris_now
        #    f["real_time_prices"] = rt_price_df

        result = rt_price_df.to_dict(orient='records')


        print('Real time data {}'.format(rt_price_df))

        print('json result {}'.format(result))


        return result, 200


stock_prices_request_parser = RequestParser(bundle_errors=False)

stock_prices_request_parser.add_argument("start_date", type=str, required=False,
                                        help="start date", default="")

stock_prices_request_parser.add_argument("end_date", type=str, required=False,
                                        help="end date", default="")


class StockPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, code):

        from asset_prices.prices import get_prices
        import datetime
        import time

        args = stock_prices_request_parser.parse_args()

        try:
            start_date = datetime.datetime.strptime(args['start_date'], '%Y%m%d')
        except:
            import sys
            print("Oops!", sys.exc_info()[0], "occurred.")
            start_date = None

        try:
            end_date = datetime.datetime.strptime(args['end_date'], '%Y%m%d')
        except:
            import sys
            print("Oops!", sys.exc_info()[0], "occurred.")
            end_date = None

        start_date = (datetime.date.today() + datetime.timedelta(-200)) \
            if start_date is None else start_date
        end_date = (datetime.date.today() + datetime.timedelta(+1)) \
            if end_date is None else end_date

        print(' Dates from {} to {} '.format(start_date, end_date))
        from dateutil import rrule
        df_h_p = get_prices(asset_codes=[code], ret='df',
                            start_date=datetime.datetime.combine(start_date, datetime.time.min),
                            end_date=datetime.datetime.combine(end_date, datetime.time.min),
                            )

        print(' result {}'.format(df_h_p))
        # result = df.to_json(orient='records')
        result = df_h_p.to_dict(orient='records')
        print('json result {}'.format(result))

        return result, 200
