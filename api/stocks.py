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

        df = get_universe(name=name, country=country, type=type)

        # result = df.to_json(orient='records')
        result = df.to_dict(orient='records')
        print('json result {}'.format(result))

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



stock_prices_request_parser = RequestParser(bundle_errors=False)

stock_prices_request_parser.add_argument("start_date", type=str, required=False,
                                        help="start date", default="")

stock_prices_request_parser.add_argument("end_date", type=str, required=False,
                                        help="end date", default="")


class   StockPrices(Resource):
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

        start_date = (datetime.date.today() + datetime.timedelta(-7)) \
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

