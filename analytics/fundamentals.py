#!/usr/bin/env python
"""

Prices module

"""
from datetime import date
from asset_prices.historical_prices import get_historical_data


# return  fundamentals_data for one or a list of codes example code : "BX4.PA"
# usage get_asset_returns(asset_codes="BX4.PA")  returns 1 week history for code BX4.PAA
def get_fundamentals_data(
                        asset_codes=[],
                        start_date=None,
                        end_date=None,
                        ret='json' # json, df
                      ):
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import datetime

    eod_key = "60241295a5b4c3.00921778"
    start_date = (datetime.datetime.today() + datetime.timedelta(-7))\
        if start_date is None else start_date
    end_date = (datetime.datetime.today() + datetime.timedelta(+1)) \
        if end_date is None else end_date

    collection_name = "stocks"
    db_name = "asset_analytics"

    access_db = "mongodb+srv://sngoube:jCJpZ8tG7Ms3iF0l@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    list_stocks = asset_codes if isinstance(asset_codes, list) else [asset_codes]

    res = server[db_name][collection_name].aggregate([
        {"$match": {"FullCode": {"$in": list_stocks}}},
        {"$project": {"Financials":1}
    }])
    lres = list(res)
    fin_data = dict()
    for item in lres:
        if 'Financials' in item.keys():
            fin = item['Financials']
            elt_list = ['Balance_Sheet', 'Income_Statement', 'Cash_flow']
            for elt in elt_list:
                if elt in fin.keys():
                    dk = dict()
                    dk['currency'] = fin[elt]['currency_symbol']
                    if 'quarterly' in fin[elt].keys():
                        dk['quarterly'] = list()
                        for k in fin[elt]['quarterly'].keys():
                            dt = datetime.datetime.strptime(fin[elt]['quarterly'][k]['date'], '%Y-%m-%d').date()
                            if end_date >= dt >= start_date:
                                dk['quarterly'].append(fin[elt]['quarterly'][k])
                    if 'yearly' in fin[elt].keys():
                        dk['yearly'] = list()
                        for k in fin[elt]['yearly'].keys():
                            dt = datetime.datetime.strptime(fin[elt]['yearly'][k]['date'], '%Y-%m-%d').date()
                            if end_date >= dt >= start_date:
                                dk['yearly'].append(fin[elt]['yearly'][k])
                    fin_data[elt] = dk

    server.close()
    print("Fundamental Done")
    print(format(fin_data))

    return lres


if __name__ == '__main__':

    # get_asset_returns(asset_codes=["BX4.PA", "CAC.PA"])
    import pandas as pd
    import numpy as np
    import datetime
    start_date = (datetime.date.today() + datetime.timedelta(-1000))
    end_date = (datetime.date.today() + datetime.timedelta(1))
    asset_codes = ["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"]
    jsonn = get_fundamentals_data(asset_codes=["AIR.PA"], start_date=start_date,
                                  end_date=end_date,
                                  ret='json')
    # print('jsonn = {}'.format(jsonn))