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
import numpy as np
from tools.logger import logger_get_ref

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
    logger_get_ref.info("Disconnected!")
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
    ddf2 = pd.read_csv("./Global_Stock_Exchanges.csv", sep=',', keep_default_na=False)

    # Mapping bloomberg exchange code to EOD exchange code.
    dict_list = ddf2.to_dict(orient='records')
    dict_eod_bloom = dict()
    for mp in dict_list:
        bb = mp['Bloomberg Exchange Code']
        dict_eod_bloom[bb] = {"ExchangeCode": mp['EOD code'], "Country": mp['Country']}

    # Adding composite exchange codes:
    dict_eod_bloom['GR'] = {"ExchangeCode": dict_eod_bloom['GY']['ExchangeCode'],
                            "Country": dict_eod_bloom['GY']['Country']}
    dict_eod_bloom['SW'] = {"ExchangeCode": dict_eod_bloom['SE']['ExchangeCode'],
                            "Country": dict_eod_bloom['SE']['Country']}
    dict_eod_bloom['US'] = {"ExchangeCode": dict_eod_bloom['UQ']['ExchangeCode'],
                            "Country": dict_eod_bloom['UQ']['Country']}
    dict_eod_bloom['CN'] = {"ExchangeCode": dict_eod_bloom['CT']['ExchangeCode'],
                            "Country": dict_eod_bloom['CT']['Country']}

    stock_list = [] = []
    dict_stock_prim_listing = df_stocks.to_dict(orient='records')
    for pl in dict_stock_prim_listing:
        bb = pl['Exchange']
        pl['ExchangeCode'] = dict_eod_bloom[bb]['ExchangeCode']
        pl['Country'] = dict_eod_bloom[bb]['Country']
        stock_list.append(pl)
        #list_stock_prim_listing.append('{}.{}'.format(pl['ISIN'], dict_eod_bloom[pl['Bloomberg Exchange Code']]))


    my_list_of_stocks = df_stocks['ISIN'].tolist()

    # https://eodhistoricaldata.com/api/exchange-symbol-list/{EXCHANGE_CODE}?api_token={YOUR_API_KEY}
    #country_list = ['US', 'LSE', 'XETRA', 'VI', 'MI', 'PA', 'BR', 'SW', 'LU', 'MC', 'AS', 'LS', 'ST',
    #                'CO', 'OL', 'HK', 'TA', 'SW', 'FOREX', 'FOREX', 'ETLX' , 'INDX', 'CC']

    #'ISIN','Currency','Exchange','ETF Underlying Index Ticker', 'Dvd Freq', 'Code', 'Type','Name', 'GICS Sector','GICS Ind Name'
    #'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'ISIN'

    """ 
    final_df = pd.DataFrame()
    for xcode in country_list:
        req = requests.get(
            "https://eodhistoricaldata.com/api/exchange-symbol-list/{}?api_token=60241295a5b4c3.00921778&fmt=json".format(
                xcode)
        )
        data = req.json()
        df_exchange_stocks = pd.DataFrame.from_dict(data)
        df_exchange_stocks.columns = ['Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'ISIN']
        df_exchange_stocks['ExchangeCode'] = xcode
        df_exchange_stocks.index = df_exchange_stocks['ISIN']
        final_df = final_df.append(df_exchange_stocks.loc[df_exchange_stocks['ISIN'].isin(my_list_of_stocks)])
    """
    #final_df.drop_duplicates(subset="ISIN", inplace=True)
    final_df = pd.DataFrame(stock_list)
    final_df.to_csv("./stock_universe.csv")


def is_valid_json(obj):

    try:
        stock_data = obj.json()
        tt = stock_data['General']
    except:
        logger_get_ref.info("Unable to serialize the object {}".format(obj))
        return False

    return True


def get_indx_cc_fx_universe(name="", type=""):
    import pandas as pd
    from pymongo import MongoClient

    collection_name = "indx_forex_cc"
    db_name = "asset_analytics"
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)

    query = {"Name": {"$regex": '/*{}/*'.format(name), "$options": 'i'},
             "Type": {"$regex": '/*{}/*'.format(type), "$options": 'i'}
             }

    res = server[db_name][collection_name].find(query)

    logger_get_ref.info("Query {}".format(query))

    lres = list(res)

    item_list = []
    result = []
    if len(lres) > 0:
        logger_get_ref.info(' result {}'.format(lres))
        df = pd.DataFrame(lres)
        logger_get_ref.info(' df {}'.format(df))
        df = df[['FullCode', 'Type', 'Name']]
        df['Code'] = df['FullCode'].apply(lambda x: x.split('.')[0])
        df['Country'] = ''
        df['Currency'] = ''
        df['ExchangeCode'] = df['FullCode'].apply(lambda x: x.split('.')[1])
        df['logo'] = ''
        df['ISIN'] = ''
        df['Exchange'] = ''
        df = df[['ISIN', 'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'ExchangeCode', 'logo']]
        # logger_get_ref.info(format(df.to_json(orient='records')))
        server.close()
        logger_get_ref.info(' result {}'.format(df))
        # result = df.to_json(orient='records')
        #result = df.to_dict(orient='records')
        #logger_get_ref.info('json result {}'.format(result))
    return df


def get_universe(name="", country="", type="", sector="",  skip=0, limit=5000,
                 codes="", order_type ="last_price_volume", asset_type ="", search_any="",
                 order_direction="desc"):
    import pandas as pd
    from pymongo import MongoClient, ASCENDING, DESCENDING

    order_dir = DESCENDING if order_direction == "desc" else ASCENDING
    order_type ="last_price_volume"  if order_type == "" else order_type
    #collection_name = "stock_universe"
    collection_name = "stock_data"
    db_name = "asset_analytics"
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)
    query = {}
    if len(name) > 0:
        query["General.Name"] = {"$regex": '/*{}/*'.format(name), "$options": 'i'}
    if len(country) > 0:
        query["General.CountryName"] = {"$regex": '/*{}/*'.format(country), "$options": 'i'}
    if len(sector) > 0:
        query["General.Sector"] = {"$regex": '/*{}/*'.format(sector), "$options": 'i'}
    if len(type) > 0:
        query["General.Type"] = {"$regex": '/*{}/*'.format(type), "$options": 'i'}
    if len(asset_type) > 0:
        query["AssetType"] = {"$regex": '/*{}/*'.format(asset_type), "$options": 'i'}

    query["Active"] = 1
    if len(codes) > 0 and codes != 'All':
        full_code_list = codes.split(',')
        query["FullCode"] = {"$in": full_code_list}

    '''
     query = {"General.Name": {"$regex": '/*{}/*'.format(name), "$options": 'i'},
             "Country": {"$regex": '/*{}/*'.format(country), "$options": 'i'},
             "Type.Type": {"$regex": '/*{}/*'.format(type), "$options": 'i'}
             }
    '''

    display = {'ISIN': 1,
                'Code': 1,
                'General.Name': 1,
                'Country': 1,
                'Type': 1,
                'General.Exchange': 1,
                'General.CurrencyCode': 1,
                'General.Type': 1,
                'Exchange': 1,
                'AssetType': 1,
                'Leverage': 1,
                'Short_Name': 1,
                'Short_Name_FR': 1,
                'Description': 1,
                'Description_FR': 1,
                'Index_Ticker': 1,
                'Issuer': 1,
                'Acc': 1,
                'last_price_volume': 1,
                 order_type: 1,
                'General.LogoURL': 1}

    displayt = {}

    #server[db_name][collection_name].update_many({"last_price_rt": "0"},
    #    {"$set":{"last_price_rt": 0, "last_price_volume_rt": 0, "last_price_change_p_rt": 0 }}
    #);

    logger_get_ref.info("search_any {} len ={} ".format(search_any, len(search_any)))
    if len(search_any) > 0:
        # { $or: [{ author: 'dave' }, { author: 'john' }] }
        full_code_list = search_any.split(',')

        query = {"$or": [{"Active": 1, "General.Name": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "ShortName": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "ShortNameFR": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "Description": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "DescriptionFR": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "Issuer": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "Code": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                         {"Active": 1, "ISIN": {"$regex": '/*{}/*'.format(search_any), "$options": 'i'}},
                           {"Active": 1, "FullCode": {"$in": full_code_list}}]
                  }

    query = [
        {"$match": query},
        {"$sort": {order_type: order_dir}},
        {'$facet': {
            "metadata": [{"$count": "total"}],
            "data": [{"$skip": skip},
                     {"$limit": limit},
                     {"$project": display}]
            }
        }
    ]




    logger_get_ref.info("Query get_universe {} ".format(query))

    res = server[db_name][collection_name].aggregate(query)
    ''' 
    res = server[db_name][collection_name].find(query, {'ISIN': 1,
                                                        'Code': 1,
                                                        'General.Name': 1,
                                                        'Country': 1,
                                                        'General.Exchange': 1,
                                                        'General.CurrencyCode': 1,
                                                        'General.Type': 1,
                                                        'Exchange': 1,
                                                        'last_price_volume': 1,
                                                        'General.LogoURL': 1
                                                        }).sort([('last_price_volume', DESCENDING)])
    '''
    logger_get_ref.info("Query get_universe {} ".format(query))

    output = list(res)[0]

    logger_get_ref.info("Query result {} ".format(output))
    total = 0
    if len(output['metadata']) > 0 and 'total' in output['metadata'][0]:
        total = output['metadata'][0]['total'] - 1
    lres = output['data']
    item_list = []
    result = []
    df = pd.DataFrame()
    if len(lres) > 0:
        for itx in lres:
            logger_get_ref.info("Query itx = {}".format(itx))
            at  = None if 'AssetType' not in itx.keys() else itx['AssetType']
            item_list.append({'ISIN': itx['ISIN'],
                              'Code': itx['Code'],
                              'Name': itx['General']['Name'],
                              'Country':  itx['Country'],
                              'Currency': itx['General']['CurrencyCode'],
                              'Type': itx['General']['Type'],

                              'Leverage': itx['Leverage'],
                              'ShortName': itx['Short_Name'],
                              'ShortNameFR': itx['Short_Name_FR'],
                              'Description': itx['Description'],
                              'DescriptionFR': itx['Description_FR'],
                              'Issuer': itx['Issuer'],
                              'Acc': itx['Acc'],
                              'IndexTicker': itx['Index_Ticker'],


                              'AssetType': at,
                              'ExchangeCode': itx['General']['Exchange'],
                              'Exchange': itx['General']['Exchange'],
                              'LastPriceVolume': itx['last_price_volume'] if 'last_price_volume' in itx.keys() else 0,
                              'logo':  '{}{}'.format('https://eodhistoricaldata.com', itx['General']['LogoURL'] if itx['Type'] not in ['ETP', 'ETF', 'ETC', 'ETN'] else 'https://devarteechadvisor.blob.core.windows.net/public-files/ETF.png' )
                               }
                             )

        import os
        # logger_get_ref.info(' result {}'.format(item_list))
        df = pd.DataFrame(item_list)
        # logger_get_ref.info(' df {}'.format(df))
        df = df[['ISIN', 'Acc', 'Issuer', 'Leverage', 'ShortName', 'ShortNameFR', 'Description', 'DescriptionFR',
                 'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type','AssetType', 'ExchangeCode', 'logo',
                 'IndexTicker', 'LastPriceVolume']]
        #df['EsgScore'] = np.random.randint(150, 255, size=len(df))
        df['IndustryAverageEsgScore'] = np.random.randint(150, 800, size=len(df))
        df['Environmental'] = np.random.randint(150, 255, size=len(df))
        df['Social'] = np.random.randint(150, 255, size=len(df))
        df['Governance'] = np.random.randint(150, 255, size=len(df))

        df['EnvironmentalNote'] = 'BBB'
        df['SocialNote'] = 'BBB'
        df['GovernanceNote'] = 'AAA'

        df['EsgScore'] = df['Environmental'] + df['Social'] + df['Governance']
        df['EsgScoreNote'] = 'BBB'
        # df['logo'] = df.apply(lambda row: row['logo'] if row['Type'] not in ['ETP', 'ETF', 'ETC', 'ETN'] else 'https://devarteechadvisor.blob.core.windows.net/public-files/ETF.png', axis = 1 )
        # logger_get_ref.info(format(df.to_json(orient='records')))

        df['total_stocks'] = total

    server.close()
        #logger_get_ref.info(' result {}'.format(df))

    logger_get_ref.info("df  result {} ".format(df))
    return df


def load_equity_etf_list():
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import os
    import re
    #jCJpZ8tG7Ms3iF0le
    eod_key = "60241295a5b4c3.00921778"
    # https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json
    ddf = pd.read_csv("../asset_prices/stock_universe_etf.csv", sep=',', keep_default_na=False)
    ddf = ddf[['ISIN', 'Code', 'Name','Country','Exchange','Currency','Type','ExchangeCode',
               'ETF Underlying Index Ticker', 'Dvd Freq', 'AssetType', 'Leverage', 'Short Name'
        ,'Short Name FR', 'Description', 'Description FR', 'Issuer', 'Acc']]

    list_stock = json.loads(ddf.to_json(orient='records'))
    collection_name = "stock_data"
    db_name = "asset_analytics"
    #req = requests.get("https://eodhistoricaldata.com/api/exchanges-list/?api_token=60241295a5b4c3.00921778&fmt=json")

    #req = requests.get("https://eodhistoricaldata.com/api/fundamentals/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json")
    #fundamental_data = req.json()

    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)
    lstock = ddf.to_dict(orient='records')

    sdict = dict()
    for ss in lstock:
        sdict['{}.{}'.format(ss['Code'], ss['ExchangeCode'])] = ss

    # lstock = lstock[0:10]


    # Need to set all the stock as disable first, then at the end only activate the one as part of the universe
    # stock_data['Status'] = 'Disable',

    #filtering = {'FullCode': cod}
    #nlstock.append(stock)
    #server[db_name][collection_name].find(filtering)
    server[db_name][collection_name].update_many({}, {'$set': {"Active":0}})
    stock_data_list = []
    ct = 1

    nlstock = []
    failedlstock = []
    import random
    # lstock = random.sample(lstock, 50) # load only 200
    total = len(lstock)
    for stock in lstock:
        cod = '{}.{}'.format(stock['Code'], stock['ExchangeCode'])
        logger_get_ref.info('Total loaded {}/{} - Getting data for sub string {}'.format(ct, total, cod))
        sreq = "https://eodhistoricaldata.com/api/fundamentals/{}?api_token=60241295a5b4c3.00921778&fmt=json".format(cod)
        logger_get_ref.info('sreq:{}'.format(sreq))
        req = requests.get(sreq)
        if is_valid_json(req):
            stock_data = req.json()
            stock_data['Code'] = stock['Code'] #stock_data['General']['Code']
            stock_data['Exchange'] = stock_data['General']['Exchange']
            stock_data['Type'] = stock['Type'] # sdict[cod]['Type']
            stock_data['Country'] = stock['Country'] # sdict[cod]['Country']
            stock_data['ISIN'] = stock['ISIN'] # sdict[cod]['ISIN']
            stock_data['FullCode'] = cod
            stock_data['AssetType'] = stock['AssetType']
            stock_data['ETF Underlying Index Ticker'] = stock['ETF Underlying Index Ticker']

            stock_data = remove_special_car(stock_data)
            stock_data_list.append(stock_data)
            stock['logo'] = '' # default logo
            stock['Market_Open'] = '9am -5pm CET'
            stock['Asset_class'] = 'Equity, France Equity, Eu Equity'

            stock_data['Index_Ticker'] = stock['ETF Underlying Index Ticker']
            stock_data['Leverage'] = stock['Leverage']
            stock_data['Short_Name'] = stock['Short Name']
            stock_data['Short_Name_FR'] = stock['Short Name FR']
            stock_data['Description'] = stock['Description']
            stock_data['Description_FR'] = stock['Description FR']
            stock_data['Issuer'] = stock['Issuer']
            stock_data['Acc'] = stock['Acc']
            stock_data['Active'] = 1

            if 'LogoURL' in stock_data['General'].keys():
                stock['logo'] = '{}{}'.format('https://eodhistoricaldata.com',stock_data['General']['LogoURL'])
                stock['logo'] = stock['logo'] if stock['Type'] not in ['ETP', 'ETF', 'ETC','ETN']\
                                    else 'https://devarteechadvisor.blob.core.windows.net/public-files/ETF.png'

            stock_data = add_translation(stock_data, field='Description',
                                         llg=['it', 'fr'])
            stock_data = add_translation(stock_data, field='Category',
                                         llg=['it', 'fr'])
            filtering = {'FullCode': cod}
            nlstock.append(stock)
            server[db_name][collection_name].find(filtering)
            server[db_name][collection_name].update_one(filtering, {'$set': stock_data}, upsert=True)
            server[db_name][collection_name].update_many(filtering, {'$set': {"Active": 1}})
            logger_get_ref.info("load data for {} completed, with query {}!".format(cod, filtering))
        else:
            failedlstock.append(stock)
        ct = ct + 1

    logger_get_ref.info("nlstock to load = {}".format(nlstock))

    collection_name = "stock_universe"
    if collection_name in server[db_name].list_collection_names():
        server[db_name][collection_name].drop()
    server[db_name][collection_name].insert_many(nlstock)

    failed_df = pd.DataFrame(failedlstock)
    failed_df.to_csv("./stock_universe_failed_load.csv")

    logger_get_ref.info("Done!")
    logger_get_ref.info("Disconnected!")
    server.close()


def add_translation(stock_data, field ='Category', llg = ['de', 'it', 'fr', 'es', 'nl', 'en']):
    if field in stock_data['General'].keys():
        text = stock_data['General'][field]
        ddescp =  {'de':text, 'it':text, 'fr':text, 'es':text, 'nl':text, 'en':text} # translate(text, llg)
        for lg in llg:
            if lg in ddescp.keys():
                stock_data['General']['{}_{}'.format(field, lg)] = ddescp[lg]
    return stock_data


def translate2(text='Hello world', llg = ['de', 'it', 'fr', 'es', 'nl', 'en']):
    import requests, uuid, json

    # Add your subscription key and endpoint
    subscription_key = "6e54709988e74470b07386b69bc42534"
    endpoint = "https://api.cognitive.microsofttranslator.com"

    # Add your location, also known as region. The default is global.
    # This is required if using a Cognitive Services resource.
    location = "francecentral"

    path = '/translate'
    constructed_url = endpoint + path

    params = {
        'api-version': '3.0',
        'from': 'en',
        'to': ['de', 'it']
    }
    constructed_url = endpoint + path

    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Ocp-Apim-Subscription-Region': location,
        'Content-type': 'application/json',
        'X-ClientTraceId': str(uuid.uuid4())
    }

    # You can pass more than one object in body.
    body = [{
        'text': 'Hello World!'
    }]

    request = requests.post(constructed_url, params=params, headers=headers, json=body)
    response = request.json()
    logger_get_ref.info(json.dumps(response, sort_keys=True, ensure_ascii=False, indent=4, separators=(',', ': ')))

def translate(text='Hello world', llg = ['de', 'it', 'fr', 'es', 'nl', 'en']):

    import requests, uuid, json
    # Add your subscription key and endpoint
    subscription_key = "6e54709988e74470b07386b69bc42534"
    endpoint = "https://api.cognitive.microsofttranslator.com/"

    # Add your location, also known as region. The default is global.
    # This is required if using a Cognitive Services resource.
    location = "francecentral"

    path = '/translate'

    chunks = [llg[x:x + 2] for x in range(0, len(llg), 2)]
    ddt = dict()
    for sllg in chunks:

        params = {
            'api-version': '3.0',
            'from': 'en',
            'to': sllg
        }
        constructed_url = endpoint + path

        headers = {
            'Ocp-Apim-Subscription-Key': 'f61d9d12813c4ed6ba2baa2dde83abff',
            'Ocp-Apim-Subscription-Region': location,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }

        # You can pass more than one object in body.
        body = [{
            'text': '{}'.format(text)
        }]

        request = requests.post(constructed_url, params=params, headers=headers, json=body)
        logger_get_ref.info("except :response{}".format(request.text))
        response = request.json()

        try:
            lelt = response[0]['translations']
            for tt in lelt:
                ddt[tt['to']] = tt['text']
        except:
            #ddt = dict()
            logger_get_ref.info("except :response{}".format(response))
        finally:
            logger_get_ref.info('{} , {}'.format(type(response), response))
            logger_get_ref.info("finally :ddt {}".format(ddt))
            #return ddt

        logger_get_ref.info('{} , {}'.format(type(response), response))
        logger_get_ref.info('{} , {}'.format(type(ddt), ddt))
    return ddt

if __name__ == '__main__':
    #load_stock_universe()
    # get_universe(name="", country="France", type="", sector="", skip=2, limit=10)
    load_equity_etf_list()
    #load_exchange_list()
    #translate(text='Hello world', llg=['de', 'it', 'fr', 'es', 'nl', 'en'])


