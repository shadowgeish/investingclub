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


def load_exchange_list():
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import os
    import re
    #jCJpZ8tG7Ms3iF0l
    eod_key = "60241295a5b4c3.00921778"
    # https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json

    collection_name = "exchanges"
    db_name = "asset_analytics"
    req = requests.get("https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json")
    list_exchange = req.json()

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    if collection_name in server[db_name].list_collection_names():
        server[db_name][collection_name].drop()
    server[db_name][collection_name].insert_many(list_exchange)
    print("Disconnected!")
    server.close()

def remove_special_car(d):
    nd = dict()
    for k, v in d.items():
        nk = k.replace('.', '')
        nd[nk] = remove_special_car(v) if isinstance(v, dict) else v
    return nd


def load_stock_universe():

    import pandas as pd
    import requests
    import json
    df_stocks = pd.read_csv("./asset_list.csv", sep=';', keep_default_na=False)
    my_list_of_stocks = df_stocks['ISIN'].tolist()

    # https://eodhistoricaldata.com/api/exchange-symbol-list/{EXCHANGE_CODE}?api_token={YOUR_API_KEY}
    country_list = ['PA', 'BE', 'LS', 'MC', 'TO', 'V', 'NEO', 'XETRA', 'VI', 'MI', 'BR', 'VX', 'US', 'LSE', 'AS', 'CO',
                    'OL', 'CC', 'CN']

    final_df = pd.DataFrame()
    for xcode in country_list:
        req = requests.get(
            "https://eodhistoricaldata.com/api/exchange-symbol-list/{}?api_token=60241295a5b4c3.00921778&fmt=json".format(
                xcode)
        )
        data = req.json()
        df_exchange_stocks = pd.DataFrame.from_dict(data)
        df_exchange_stocks.columns = ['Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'ISIN']
        df_exchange_stocks.index = df_exchange_stocks['ISIN']
        final_df = final_df.append(df_exchange_stocks.loc[df_exchange_stocks['ISIN'].isin(my_list_of_stocks)])

    final_df.drop_duplicates(subset="ISIN", inplace=True)

    final_df.to_csv("./stock_universe.csv")

def load_equity_etf_list():
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import os
    import re
    #jCJpZ8tG7Ms3iF0l
    eod_key = "60241295a5b4c3.00921778"
    # https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json
    ddf = pd.read_csv("./stock_universe.csv", sep=';', keep_default_na=False)
    list_stock = json.loads(ddf.to_json(orient='records'))
    collection_name = "stocks"
    db_name = "asset_analytics"
    #req = requests.get("https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json")

    #req = requests.get("https://eodhistoricaldata.com/api/fundamentals/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json")
    #fundamental_data = req.json()

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)
    lstock = ddf.to_dict(orient='records')
    stock_data_list = []
    print('{}'.format(lstock))
    for stock in lstock:
        code = "{}.{}".format(stock['Code'], stock['Exchange'])

        print('code:{}'.format(code))

        req = requests.get(
            "https://eodhistoricaldata.com/api/fundamentals/{}?api_token=60241295a5b4c3.00921778&fmt=json".format(code)
        )
        print('req:{}'.format(req))
        try:
            stock_data = req.json()
            stock_data['Code'] = stock['Code']
            stock_data['Exchange'] = stock['Exchange']
            stock_data['Type'] = stock['Type']
            stock_data['Dvd Freq'] = stock['Dvd Freq']
            stock_data['ISIN'] = stock['ISIN']
            stock_data['FullCode'] = code
            stock_data = remove_special_car(stock_data)
            stock_data_list.append(stock_data)
            print(" load data for {} completed!".format(code))
        except:
            pass

    if collection_name in server[db_name].list_collection_names():
        server[db_name][collection_name].drop()
    server[db_name][collection_name].insert_many(stock_data_list)
    print("Done!")
    print("Disconnected!")
    server.close()


if __name__ == '__main__':
    #load_stock_universe()
    load_equity_etf_list()
    # load_exchange_list()

