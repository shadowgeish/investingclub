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

stock_universe_request_parser.add_argument("stock_type", type=str, required=False,
                                        help="stock type", default="")

stock_universe_request_parser.add_argument("sector", type=str, required=False,
                                        help="sector", default="")

stock_universe_request_parser.add_argument("cache", type=str, required=False,
                                        help="cache", default="false")

stock_universe_request_parser.add_argument("skip", type=int, required=False,
                                        help="skip", default=0)

stock_universe_request_parser.add_argument("limit", type=int, required=False,
                                        help="limit", default=10)

stock_universe_request_parser.add_argument("candle", type=int, required=False,
                                        help="candle", default=1)

stock_universe_request_parser.add_argument("lastpriceonly", type=int, required=False,
                                        help="lastpriceonly", default=0)

stock_universe_request_parser.add_argument("start_date", type=str, required=False,
                                                 help="start date", default="")

stock_universe_request_parser.add_argument("end_date", type=str, required=False,
                                         help="end date", default="")

stock_universe_request_parser.add_argument("historical", type=int, required=False,
                                         help="historical", default=0)

stock_universe_request_parser.add_argument("order_type", type=str, required=False,
                                         help=" order_type", default="")

stock_universe_request_parser.add_argument("order_direction", type=str, required=False,
                                         help=" order_direction", default="")

class HelloWord(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self):
        return {"about": "Hello world"}, 200

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
        stock_type = args['stock_type']
        sector = args['sector']
        skip = args['skip']
        limit = args['limit']
        order_type = args['order_type']
        order_direction = args['order_direction']

        df = get_universe(name=name, country=country, type=stock_type, sector=sector, skip=skip, limit=limit,
                          order_type=order_type, order_direction=order_direction)

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

def get_date_from_str_or_default(datestr, default_date_obj):
    from datetime import datetime
    try:
        date_obj = datetime.datetime.strptime(datestr, '%Y%m%d')
    except:
        import sys
        print("Oops!", sys.exc_info()[0], "occurred.")
        date_obj = default_date_obj
    return date_obj


class StockDataAndPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, codes):
        import pandas as pd
        from datetime import datetime
        import pytz
        args = stock_universe_request_parser.parse_args()
        name = args['name']
        country = args['country']
        stock_type = args['stock_type']
        sector = args['sector']
        skip = args['skip']
        limit = args['limit']
        historical = args['historical']
        order_type = args['order_type']
        order_direction = args['order_direction']

        tz = pytz.timezone('Europe/Paris')

        #if codes == 'All':
        df = get_universe(name=name, country=country, type=stock_type,
                          sector=sector, skip=skip, limit=limit,
                          codes=codes, order_type =order_type, order_direction=order_direction)
        dict_stock = OrderedDict()
        if len(df) > 0:
            df['full_code'] = df['Code'] + '.' + df['ExchangeCode']
            lstock = df['full_code'].tolist()
            universe = df.to_dict(orient='records')
            print('No Data returned!!')
        else:
            return  dict_stock, 200

        s_now = datetime.now(tz)

        from asset_prices.prices import get_prices
        paris_now = datetime.now(tz)
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')
        fetch_type = 'real_time'
        #  check if day is the week-end
        if start_date.weekday() > 4 or historical==1:
            import datetime as dte
            start_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + dte.timedelta(-40)
            end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M') + dte.timedelta(+1)
            fetch_type = 'historical'

        for stock in universe:
            dict_stock[stock['Code'] + '.' + stock['ExchangeCode']] = stock
            dict_stock[stock['Code'] + '.' + stock['ExchangeCode']]['price_frequency'] = fetch_type

        rt_price_df = get_prices(asset_codes=lstock, start_date=start_date, end_date=end_date,
                                 type=fetch_type, ret_code=1, ret='df')

        result = rt_price_df.to_dict(orient='records')

        for price in result:
            if 'prices' not in dict_stock[price['code']].keys():
                dict_stock[price['code']]['prices'] = list()
            dict_stock[price['code']]['prices'].append(price)

        print('RAN in {}!!'.format((s_now - datetime.now(tz)).total_seconds()))


        return dict_stock, 200

class StockUniverseLastPrice(Resource):
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

        # if codes == 'All':
        df = get_universe(name=name, country=country, type=type, sector=sector, skip=skip, limit=limit, codes=codes)
        df['full_code'] = df['Code'] + '.' + df['ExchangeCode']
        lstock = df['full_code'].tolist()
        universe = df.to_dict(orient='records')

        print('READ FROM THE DATABASE!!')
        s_now = datetime.now(tz)

        from asset_prices.prices import get_prices
        paris_now = datetime.now(tz)
        start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
        end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')

        rt_price_df = get_prices(asset_codes=lstock, start_date=start_date, end_date=end_date,
                                 type='real_time', ret_code=1, ret='df')

        if 'volume' in rt_price_df.columns:
            rt_price_df = rt_price_df.sort_values(by=['volume'], ascending=False)

        result = rt_price_df.to_dict(orient='records')

        dict_prices = OrderedDict()
        for price in result:
            if price['code'] not in dict_prices.keys():
                dict_prices[price['code']] = price
            if price['converted_date'] > dict_prices[price['code']]['converted_date']:
                dict_prices[price['code']] = price

        return dict_prices, 200



class StockPrices(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)
    def get(self, codes):
        import pandas as pd
        from datetime import datetime
        import pytz
        args = stock_universe_request_parser.parse_args()
        name = args['name']
        country = args['country']
        stock_type = args['stock_type']
        sector = args['sector']
        cache = args['cache']
        skip = args['skip']
        limit = args['limit']
        candle = args['candle']
        lastpriceonly = args['lastpriceonly']
        historical = args['historical']
        order_type = args['order_type']
        order_direction = args['order_direction']

        tz = pytz.timezone('Europe/Paris')

        # if codes == 'All':
        df = get_universe(name=name, country=country, type=stock_type, sector=sector, skip=skip, limit=limit,
                          codes=codes, order_type=order_type, order_direction=order_direction)
        df['full_code'] = df['Code'] + '.' + df['ExchangeCode']
        lstock = df['full_code'].tolist()
        universe = df.to_dict(orient='records')
        # else:
        #    lstock = codes.split(',')
        #    universe = lstock

        s_now = datetime.now(tz)

        from asset_prices.prices import get_prices
        if historical == 1:

            start_date = get_date_from_str_or_default(args['start_date'],
                                                      (datetime.date.today() + datetime.timedelta(-200)))
            end_date = get_date_from_str_or_default(args['end_date'],
                                                      (datetime.date.today() + datetime.timedelta(1)))
        else:
            paris_now = datetime.now(tz)
            start_date = datetime.strptime(paris_now.strftime("%d%m%Y0700"), '%d%m%Y%H%M')
            end_date = datetime.strptime(paris_now.strftime("%d%m%Y2300"), '%d%m%Y%H%M')

        rt_price_df = get_prices(asset_codes=lstock, start_date=start_date, end_date=end_date,
                                 type='real_time', ret_code=1, ret='df')

        if 'volume' in rt_price_df.columns:
            rt_price_df = rt_price_df.sort_values(by=['volume'], ascending = False )

        result = rt_price_df.to_dict(orient='records')

        dict_prices = OrderedDict()

        if lastpriceonly == 1:
            for price in result:
                if price['code'] not in dict_prices.keys():
                    dict_prices[price['code']] = price
                if price['converted_date'] > dict_prices[price['code']]['converted_date']:
                    dict_prices[price['code']] = price

        elif candle == 0:
            for price in result:
                if price['code'] not in dict_prices.keys():
                    dict_prices[price['code']] = list()
                dict_prices[price['code']].append({'code': price['code'],
                                                   'timestamp': price['timestamp'],
                                                   'volume': price['volume'],
                                                   'change_p': price['change_p'],
                                                   'close': price['close']})
        else:
            for price in result:
                if price['code'] not in dict_prices.keys():
                    dict_prices[price['code']] = list()
                dict_prices[price['code']].append(price)

        print('READ FROM THE DATABASE in {}!!'.format((s_now - datetime.now(tz)).total_seconds()))

        return dict_prices, 200



stock_data_request_parser = RequestParser(bundle_errors=False)

stock_data_request_parser.add_argument("lang", type=str, required=False,
                                        help="lang", default="en")

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

        args = stock_data_request_parser.parse_args()
        lang = args['lang']

        lg_list = ['es', 'fr', 'nl', 'it', 'en', 'de']

        print('lg_list = {}'.format(lg_list))
        if lang in lg_list:
            lg_list.remove(lang)

        print('New lg_list = {}'.format(lg_list))

        filter = {'_id': 0, 'ETF_Data.Market_Capitalisation': 0,
                             'General.Officers': 0,
                             'General.Listings': 0,
                            'General.Description': 0,
                             'outstandingShares': 0,
                             'Valuation': 0,
                             'Financials': 0,
                             'Earnings': 0,
                             'ESGScores': 0,
                             'prices': 0,
                             'InsiderTransactions':0,
                             'SplitsDividends': 0,
                             'SharesStats': 0
                             }

        for lg in lg_list:
            filter['General.Description_' + lg] = 0

        print(' filter = {}'.format(filter))

        # code = 'FR.PA'
        collection_name = "stock_data"
        db_name = "asset_analytics"
        access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
        server = MongoClient(access_db)
        # ESGScores, Earnings, Financials, SharesStats, SplitsDividends, Valuation, outstandingShares, General.Officers, General.Listings
        query = {"FullCode": code}
        res = server[db_name][collection_name].find_one(query, filter)

        res2 = server[db_name]["stock_esg_data"].find_one(query, {'_id': 0})

        json_res = {} if res is None else json.loads(json_util.dumps(res))
        json_res2 = {} if res2 is None else json.loads(json_util.dumps(res2))

        merged_dict = dict(json_res, **json_res2)

        #print("Query {} and result {}".format(query, merged_dict))

        return merged_dict, 200



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


stock_prices_request_parser = RequestParser(bundle_errors=False)

stock_prices_request_parser.add_argument("start_date", type=str, required=False,
                                        help="start date", default="")

stock_prices_request_parser.add_argument("end_date", type=str, required=False,
                                        help="end date", default="")


class StockPricesOld(Resource):
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
