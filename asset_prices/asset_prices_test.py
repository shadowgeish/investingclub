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


def get_exchange_list():
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import os
    import re

    eod_key = "60241295a5b4c3.00921778"
    # https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json

    collection_name = "exchanges"
    db_name = "asset_analytics"
    req = requests.get("https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json")
    list_exchange = req.json()

    access_db = "mongodb+srv://sngoube:Came1roun*@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    if collection_name in server[db_name].list_collection_names():
        server[db_name][collection_name].drop()
    server[db_name][collection_name].insert_many(list_exchange)
    print("Disconnected!")
    server.close()


def get_historical_data(asset_ticker=None,
                        start_date=None,
                        end_date=None,
                        verbose=False):
    import pandas as pd
    import requests
    """
            return a dataframe of historical prices

            # IWVCPPHPKILWRYGB

        """
    if asset_ticker is None:
        asset_ticker = ['IBM', 'AAPL', 'MSFT']  # IBM, Apple, Microsoft

    key = "IWVCPPHPKILWRYGB"

    for company in asset_ticker:
        data = requests.get(
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + company + '&outputsize=full&apikey={}'.format(
                key))
        jdata = data.json()

        company_name = jdata["Meta Data"]["2. Symbol"]
        daily = jdata['Time Series (Daily)']
        list_daily = []

        for date, value in daily.items():
            d = {}
            d['Company'] = company_name
            d['Date'] = date
            d['Open'] = value['1. open']
            d['High'] = value['2. high']
            d['Low'] = value['3. low']
            d['Close'] = value['4. close']
            d['Volume'] = value['5. volume']
            list_daily.append(d)

        # pd.DataFrame(list_daily).to_json('{}.json'.format(company_name), orient='records', date_format="iso")
        return list_daily
        # return pd.DataFrame(list_daily)


def asset_prices_load_history(asset_ticker=None,
                              start_date=None,
                              end_date=None,
                              full=False,
                              verbose=False):
    """
    bulck insert the entire history

    """
    from pymongo import MongoClient
    import json
    import os
    import re
    accessdb = "mongodb://arteech-dev:cgKpbDj0YvfzaumUtem03P2GYQgPUVSCAYImvRsLbnTN01c9ZgOziCxbvDsFcyCY81J2WhFUVmc3JIOmxT9pJw==@arteech-dev.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@arteech-dev@"
    # Life table

    # accessdb = "mongodb+srv://sngoube:Came1roun*@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"

    companies = ['IBM', 'AAPL', 'MSFT']  # IBM, Apple, Microsoft

    # ****Connect to MongoDB Atlas****
    # accessdb = 'mongodb+srv://{}:{}@{}'.format(username, psswd, link)
    try:
        server = MongoClient(accessdb)
    except:
        print('log Could not connect to MongoDB :(')

    for company_ticker in companies:

        list_data = get_historical_data(asset_ticker=company_ticker,
                                        start_date=start_date,
                                        end_date=end_date,
                                        verbose=verbose)
        if full is True:
            if company_ticker in server['prices'].list_collection_names():
                server['prices'][company_ticker].drop()
            server['prices'][company_ticker].insert_many(list_data)
            print("Disconnected!")
            server.close()
            return
        # last_inserted = server['prices'][company_ticker].find({'Date': df_data.iloc[0][1]})

        # if last_inserted.retrieved > 0:
        #    pass

        # if start_date is not None and start_date == end_date:

        #    server['prices'][company_ticker].insert_one(
        #        {"Company": company_ticker, "Date": df_data.iloc[0][0].strftime("%Y-%m-%d"), "Open": df_data.iloc[0][1],
        #         "High": df_data.iloc[0][2], "Low": df_data.iloc[0][3], "Close": df_data.iloc[0][4],
        #         "Volume": df_data.iloc[0][5]})

        print("Data inserted")


asset_prices_load_history()

