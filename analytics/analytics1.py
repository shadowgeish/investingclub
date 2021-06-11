#!/usr/bin/env python
"""

Prices module

"""
from datetime import date
from asset_prices.historical_prices import get_historical_data


# return historical data for one or a list of codes example code : "BX4.PA"
# usage get_historical_data(asset_codes="BX4.PA")  returns 1 week history for code BX4.PA
# usage get_historical_data(asset_codes=["BX4.PA", "CAC.PA"])  returns 1 week history for code BX4.PA and CAC.PA
def get_asset_returns(asset_codes=[],
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

    ac = [asset_codes] if not isinstance(asset_codes, list) else asset_codes

    df = get_historical_data(asset_codes=ac, start_date=start_date, end_date=end_date, ret='df')

    retdf = pd.DataFrame()
    for asset_code in ac:
        ndf = df[df['code']==asset_code].copy()
        ndf = ndf.sort_values(by=['converted_date'])
        ndf['pct_change'] = ndf['adjusted_close'].pct_change()
        retdf = retdf.append(ndf).dropna()

    print("Analytics Done")
    print(format(retdf))

    if ret == 'json':
        return retdf.to_json(orient='records')
    else:
        return retdf


def back_test_portfolio(
                        initial_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        target_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        invested_amount = 10000,
                        rebalancing_frequency='monthly', # 'daily', 'weekly', 'yearly'
                        benchmark_list=[], # list of benchmark index code
                        contribution = {}, #   amount :value, frequency : value ('monthly', 'yearly'),
                        withdraw = {}, #  dict contribution amount : frequency ('monthly', 'yearly'),
                        start_date=None,
                        end_date=None,
                        ret='json' # json, df
                        ):
    import pandas as pd
    import requests
    from pymongo import MongoClient
    import json
    import numpy as np
    import datetime
    import time
    start_time = time.time()

    start_date = (datetime.date.today() + datetime.timedelta(-7)) \
        if start_date is None else start_date
    end_date = (datetime.date.today() + datetime.timedelta(+1)) \
        if end_date is None else end_date

    asset_codes_weight = initial_asset_codes_weight.copy()
    asset_codes = list(asset_codes_weight.keys())

    from dateutil import rrule
    df_h_p = get_historical_data(asset_codes=asset_codes, ret='df',
                                 start_date=datetime.datetime.combine(start_date, datetime.time.min),
                                 end_date=datetime.datetime.combine(end_date, datetime.time.min),
                                 )
    date_l = rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date)
    df_full = pd.DataFrame(index=date_l)
    df_full[['unbalanced_portfolio_value', 'balanced_portfolio_value', 'benchmark_value', 'benchmark_nb_share',
             'benchmark_close', 'benchmark_return', 'unbalanced_portfolio_return', 'balanced_portfolio_return',
             'has_portfolio_been_rebalanced', 'available_cash', 'portfolio_contribution', 'portfolio_withdraw'
             ]] = 0
    # ['{}_unbalanced_weight'.format(ac) for ac in asset_codes]
    for ac in asset_codes:
        data = df_h_p[df_h_p['code']==ac].copy().sort_values(by=['converted_date'])
        temp_df = pd.DataFrame(data=data['adjusted_close'].tolist(),
                               index=data['date'],
                               columns=['{}_adjusted_close'.format(ac)]
                               )
        temp_df['{}_return'.format(ac)] = temp_df['{}_adjusted_close'.format(ac)].pct_change()
        temp_df.loc[temp_df.index[0], '{}_return'.format(ac)] = 0 # keeping the first row
        temp_df[['{}_unbalanced_weight'.format(ac), '{}_balanced_weight'.format(ac),
                 '{}_unbalanced_nb_shares'.format(ac), '{}_balanced_nb_shares'.format(ac),
                 '{}_unbalanced_value'.format(ac), '{}_balanced_value'.format(ac)
                ]] = np.NaN

        df_full = df_full.join(temp_df, how="outer")
        df_full = df_full[df_full['{}_return'.format(ac)].notna()]

    # initialisation
    tdt = df_full.index[0] #datetime.datetime.combine(start_date, datetime.time.min)
    df_full.loc[tdt, 'unbalanced_portfolio_value'] = invested_amount
    df_full.loc[tdt, 'balanced_portfolio_value'] = invested_amount
    for ac in asset_codes:
        df_full.loc[tdt, '{}_unbalanced_weight'.format(ac)] = asset_codes_weight[ac]
        df_full.loc[tdt, '{}_balanced_weight'.format(ac)] = asset_codes_weight[ac]
        df_full.loc[tdt, '{}_unbalanced_nb_shares'.format(ac)] = (invested_amount * asset_codes_weight[ac]) / df_full.loc[tdt]['{}_adjusted_close'.format(ac)]
        df_full.loc[tdt, '{}_balanced_nb_shares'.format(ac)] = df_full.loc[tdt]['{}_unbalanced_nb_shares'.format(ac)]
        df_full.loc[tdt, '{}_unbalanced_value'.format(ac)] = df_full.loc[tdt]['{}_unbalanced_nb_shares'.format(ac)] * df_full.loc[tdt]['{}_adjusted_close'.format(ac)]
        df_full.loc[tdt, '{}_balanced_value'.format(ac)] = df_full.loc[tdt]['{}_unbalanced_value'.format(ac)]
        df_full.loc[tdt, 'unbalanced_portfolio_return'] = df_full.loc[tdt, 'unbalanced_portfolio_return'] + (
                asset_codes_weight[ac] * df_full.loc[tdt, '{}_return'.format(ac)])
        df_full.loc[tdt, 'balanced_portfolio_return'] = df_full.loc[tdt, 'balanced_portfolio_return'] + (
                    asset_codes_weight[ac] * df_full.loc[tdt, '{}_return'.format(ac)])

    dt_l = list(df_full.index)
    dt_l.remove(df_full.index[0])
    prev_dt = df_full.index[0]
    prev_rebal = df_full.index[0]
    prev_contrib = df_full.index[0]
    prev_withdraw = df_full.index[0]
    freq_inter = {'daily': 1, 'monthly': 22, 'quaterly' : 66, 'yearly': 264}

    for index, row in df_full.iterrows():
        if index == df_full.index[0]:
            continue

        row['available_cash'] = df_full.loc[prev_dt, 'available_cash']
        for ac in asset_codes:
            row['{}_balanced_nb_shares'.format(ac)] = df_full.loc[prev_dt, '{}_balanced_nb_shares'.format(ac)]
            row['{}_unbalanced_nb_shares'.format(ac)] = df_full.loc[prev_dt, '{}_unbalanced_nb_shares'.format(ac)]
            row['{}_unbalanced_value'.format(ac)] = row['{}_adjusted_close'.format(ac)] * \
                                                                 row['{}_unbalanced_nb_shares'.format(ac)]
            row['{}_balanced_value'.format(ac)] = row['{}_adjusted_close'.format(ac)] * \
                                                                 row['{}_balanced_nb_shares'.format(ac)]
            row['unbalanced_portfolio_return'] += (row['{}_unbalanced_weight'.format(ac)]
                                                   * row['{}_return'.format(ac)])
            row['balanced_portfolio_return'] += (row['{}_balanced_weight'.format(ac)] *
                                                  row['{}_return'.format(ac)])
            row['unbalanced_portfolio_value'] += row['{}_unbalanced_value'.format(ac)]
            row['balanced_portfolio_value'] += row[ '{}_balanced_value'.format(ac)]

        for ac in asset_codes: # We rebalance here if needed, base on frequency, % deviation
            row['{}_unbalanced_weight'.format(ac)] = row['{}_unbalanced_value'.format(ac)]\
                                                                  /row['unbalanced_portfolio_value']
            row['{}_balanced_weight'.format(ac)] = row['{}_balanced_value'.format(ac)] \
                                                                  / row['balanced_portfolio_value']
            if (index - prev_rebal).days == freq_inter[rebalancing_frequency] and bool(target_asset_codes_weight):
                if row['{}_balanced_weight'.format(ac)] > target_asset_codes_weight[ac]:
                    perc_to_rebal = row['{}_balanced_weight'.format(ac)] - target_asset_codes_weight[ac]
                    cash_associated = perc_to_rebal * row['balanced_portfolio_value']
                    number_share_to_sell = int(cash_associated / row['{}_adjusted_close'.format(ac)]) # selling
                    if number_share_to_sell > 1:
                        cash_received = number_share_to_sell * row['{}_adjusted_close'.format(ac)]
                        row['{}_balanced_nb_shares'.format(ac)] -= number_share_to_sell
                        row['available_cash'] += cash_received
                        row['has_portfolio_been_rebalanced'] = 1
                if row['{}_balanced_weight'.format(ac)] < target_asset_codes_weight[ac]:
                    perc_to_rebal = row['{}_balanced_weight'.format(ac)] - target_asset_codes_weight[ac]
                    cash_associated = perc_to_rebal * row['balanced_portfolio_value']
                    number_share_to_sell = int(cash_associated / row['{}_adjusted_close'.format(ac)]) # selling
                    if number_share_to_sell > 1:
                        cash_received = number_share_to_sell * row['{}_adjusted_close'.format(ac)]
                        row['{}_balanced_nb_shares'.format(ac)] -= number_share_to_sell
                        row['available_cash'] += cash_received
                        row['has_portfolio_been_rebalanced'] = 1

                prev_rebal = index

            # set df_full['has_portfolio_been_rebalanced'] to 1 if rebalancing.
            # check when was the last one, if the frequency to reached, rebalance.


        # do the contribution or withdraw here.
        # compare the date with the last rebalancing date. if the frequency is reached, then rebalance.
        # df_full['portfolio_contribution'] = 0 set the contribution amount here.
        # df_full['portfolio_withdraw']
        prev_dt = index

    print("backtesting from {} to {} done".format(start_date, end_date))
    print(format(df_full))
    print("--- %s seconds ---" % (time.time() - start_time))

    if ret == 'json':
        return df_full.to_json(orient='records')
    else:
        return df_full



if __name__ == '__main__':

    # get_asset_returns(asset_codes=["BX4.PA", "CAC.PA"])
    import pandas as pd
    import numpy as np
    import datetime
    start_date = (datetime.date.today() + datetime.timedelta(-3500))
    end_date = (datetime.date.today() + datetime.timedelta(1))
    df =  back_test_portfolio(
        initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
        start_date=start_date,
        end_date=end_date,
        ret='df'
    )

    df.to_csv('/Users/sergengoube/PycharmProjects/investingclub/extract_{}.csv'.format(datetime.datetime.now()))