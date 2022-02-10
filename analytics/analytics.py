#!/usr/bin/env python
"""

Prices module

"""
from datetime import date
from asset_prices.prices import get_prices
from api.utils import get_asset_type_grouping
from tools.logger import logger_get_analytics
from asset_prices.referencial import get_universe
from api.utils import get_date_from_str_or_default
from pypfopt import base_optimizer


# return historical data for one or a list of codes example code : "BX4.PA"
# usage get_historical_data(asset_codes="BX4.PA")  returns 1 week history for code BX4.PA
# usage get_historical_data(asset_codes=["BX4.PA", "CAC.PA"])  returns 1 week history for code BX4.PA and CAC.PA
def get_asset_returns(
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

    ac = [asset_codes] if not isinstance(asset_codes, list) else asset_codes

    df = get_prices(asset_codes=ac, start_date=start_date, end_date=end_date, ret='df')

    retdf = pd.DataFrame()
    for asset_code in ac:
        ndf = df[df['code']==asset_code].copy()
        ndf = ndf.sort_values(by=['converted_date'])
        ndf['pct_change'] = ndf['adjusted_close'].pct_change()
        retdf = retdf.append(ndf).dropna()

    logger_get_analytics.info("Analytics Done")
    logger_get_analytics.info(format(retdf))

    if ret == 'json':
        return retdf.to_json(orient='records')
    else:
        return retdf


def get_freq(rebalancing_frequency, contribution, withdraw, init):
    map_days = { 'day':1, 'weekly':7, 'monthly':30, 'quaterly':90, 'yearly':365 }
    map_days_l = {'day': 'D', 'weekly': 'W', 'monthly': 'M', 'quaterly': 'Q', 'yearly': 'Y'}
    current_gap = map_days[init]
    current_gap_l = map_days_l[init]

    if current_gap > map_days[rebalancing_frequency]:
        current_gap_l = map_days_l[rebalancing_frequency]
    if bool(contribution) and  current_gap > map_days[contribution['freq']]:
        current_gap_l = map_days_l[contribution['freq']]
    if bool(withdraw) and current_gap > map_days[withdraw['freq']]:
        current_gap_l = map_days_l[contribution['freq']]

    return current_gap_l


def update_row(row, dict_nb_share, contribution, withdraw, rebal, target_weight):
    import numpy as np
    asset_codes = list(dict_nb_share.keys())
    row['portfolio_value'] = 0
    row['portfolio_return'] = 0
    map_days = {'day': 1, 'weekly': 7, 'monthly': 30, 'quaterly': 90, 'yearly': 365 }

    for ac in asset_codes:
        row['{}_value'.format(ac)] = dict_nb_share[ac] * row['{}_adjusted_close'.format(ac)]
        row['portfolio_value'] += row['{}_value'.format(ac)]

    for ac in asset_codes:
        row['{}_weight'.format(ac)] = (dict_nb_share[ac] * row['{}_adjusted_close'.format(ac)])/row['portfolio_value']
        row['portfolio_return'] += (row['{}_weight'.format(ac)] * row['{}_return'.format(ac)])
        # adding contribution
        # logger_get_analytics.info('diff {}, freq:{}'.format((contribution['last'] - row.name).days, map_days[contribution['freq']]))
        if 'freq' in contribution.keys() and map_days[contribution['freq']] <= (row.name - contribution['last']).days:
            _nb = (row['{}_weight'.format(ac)] * contribution['amount'] ) / row['{}_adjusted_close'.format(ac)]
            dict_nb_share[ac] += np.round(_nb, 2)
            row['{}_nb_shares'.format(ac)] = dict_nb_share[ac]
            contribution['last'] = row.name if asset_codes[-1] == ac else contribution['last']
            #logger_get_analytics.info('added {} contribution to portfolio  on {}. nb share added : {} for {}'.format(contribution['amount'],
            #                                                                                     row.name, _nb, ac))

        #  withdraw
        if 'freq' in withdraw.keys() and map_days[withdraw['freq']] <= (row.name - withdraw['last'] ).days:
            _nb = (row['{}_weight'.format(ac)] * contribution['amount'] ) / row['{}_adjusted_close'.format(ac)]
            dict_nb_share[ac] -= np.round(_nb, 2)
            # dict_nb_share[ac] = max(dict_nb_share[ac], 0)
            row['{}_nb_shares'.format(ac)] = dict_nb_share[ac]
            withdraw['last'] = row.name if asset_codes[-1] == ac else withdraw['last']
            #logger_get_analytics.info('withdraw {} to portfolio  on {}. nb share added : {} for {}'.format(contribution['amount'], row.name,
            #      _nb, ac ))

    # rebal
    for ac in asset_codes:
        if bool(target_weight) and map_days[rebal['freq']] <= (row.name - rebal['last']).days:
                if np.abs(row['{}_weight'.format(ac)] - target_weight[ac]) >= 0.001:
                    _w_diff = (target_weight[ac] - row['{}_weight'.format(ac)])
                    _amount = (_w_diff * row['portfolio_value'])
                    _nb = _amount / row['{}_adjusted_close'.format(ac)]
                    dict_nb_share[ac] += np.round(_nb, 2)
                    row['{}_nb_shares'.format(ac)] = dict_nb_share[ac]
                    rebal['last'] = row.name if asset_codes[-1] == ac else rebal['last']
                    #logger_get_analytics.info('rebalancing to portfolio  on {}. nb share added {} : {} for {}, diff = {} ({}-{}), amount={}, price = {}'.format(
                    #    row.name, row['{}_nb_shares'.format(ac)],_nb, ac, _w_diff, row['{}_weight'.format(ac)],
                    #    target_weight[ac],_amount, row['{}_adjusted_close'.format(ac)]))

    return row


def simulate_data(df, asset_codes, forecasts, starting_price):
    for ac in asset_codes:
        df['{}_return'.format(ac)] = df['{}_return'.format(ac)].sample(n=forecasts, replace=True).values
        relative_returns = (df['{}_return'.format(ac)] + 1).cumprod()
        reverse_prices = starting_price[ac] * relative_returns
        df['{}_adjusted_close'.format(ac)] = reverse_prices
    return df


def simul_func(args):
    (last_idx, contribution, withdraw, rebal, df_full, asset_codes, forecasts, starting_price,
     dict_nb_share, target_asset_codes_weight, dffull) = args
    contribution['last'] = last_idx
    withdraw['last'] = last_idx
    rebal['last'] = last_idx
    dffull = simulate_data(dffull, asset_codes, forecasts, starting_price)
    dffull = df_full.apply(update_row,
                            args=(dict_nb_share, contribution, withdraw, rebal, target_asset_codes_weight), axis=1)
    return dffull['portfolio_return']

def monte_carlo_portfolio_simul(
                        initial_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        target_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        invested_amount = 10000,
                        rebalancing_frequency='monthly', #  'yearly', 'monthly', 'quaterly', 'weekly'
                        contribution = {}, # dict  amount :value, frequency : value ('monthly', 'yearly'),
                        withdraw = {}, #  dict contribution amount : frequency ('monthly', 'yearly'),
                        start_date=None,
                        end_date=None,
                        multi_process = False,
                        nb_simul = 1000,
                        ret='json'
                        ):
    import pandas as pd
    # import modin.pandas as pd
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
    df_h_p = get_prices(asset_codes=asset_codes, ret='df',
                                 start_date=datetime.datetime.combine(start_date, datetime.time.min),
                                 end_date=datetime.datetime.combine(end_date, datetime.time.min),
                                 )

    logger_get_analytics.info("Getting data --- %s seconds ---" % (time.time() - start_time))

    # ['{}_unbalanced_weight'.format(ac) for ac in asset_codes]
    df_full = None
    dict_nb_share = dict()
    starting_price = dict()
    last_event = dict()
    freq = get_freq(rebalancing_frequency, contribution, withdraw, 'monthly')

    df_h_p = df_h_p.dropna()

    for ac in asset_codes:
        data = df_h_p[df_h_p['code']==ac].copy().sort_values(by=['converted_date'])
        temp_df = pd.DataFrame(data=data['adjusted_close'].tolist(),
                               index=data['date'],
                               columns=['{}_adjusted_close'.format(ac)]
                               )
        temp_df['date'] = pd.to_datetime(temp_df.index)
        temp_df = temp_df.resample(freq, on='date').last()

        #temp_df['date'] = pd.to_datetime(temp_df.index)

        temp_df['{}_return'.format(ac)] = temp_df['{}_adjusted_close'.format(ac)].pct_change()
        temp_df.loc[temp_df.index[0], '{}_return'.format(ac)] = 0 # keeping the first row
        temp_df[['{}_weight'.format(ac), '{}_nb_shares'.format(ac), '{}_value'.format(ac) ]] = np.NaN
        dict_nb_share[ac] = (initial_asset_codes_weight[ac] * invested_amount) / temp_df.loc[temp_df.index[0], '{}_adjusted_close'.format(ac)]
        starting_price[ac] = temp_df.loc[temp_df.index[0], '{}_adjusted_close'.format(ac)]
        df_full = pd.DataFrame(index=temp_df.index) if df_full is None else df_full
        df_full = pd.merge(df_full, temp_df, left_index=True, right_index=True) #df_full.join(temp_df, how="outer")
        df_full = df_full[df_full['{}_return'.format(ac)].notna()]

    logger_get_analytics.info("--- end init %s seconds ---" % (time.time() - start_time))
    df_full['portfolio_value'] = 0
    df_full['portfolio_return'] = 0
    rets = pd.DataFrame(index=df_full.index)
    forecasts = len(df_full['portfolio_return'])
    rebal = dict()
    simul = nb_simul
    rebal['freq'] = rebalancing_frequency
    logger_get_analytics.info("--- start simul for {}---".format(simul))

    var_tuple = (temp_df.index[0], contribution, withdraw, rebal, df_full, asset_codes, forecasts, starting_price,
                 dict_nb_share, target_asset_codes_weight, df_full)

    # Using multi process
    if multi_process:
        import multiprocessing as mp
        with mp.Pool(processes=mp.cpu_count()) as p:
            logger_get_analytics.info("--- start simul for {}--- and  using {} cpu".format(simul, mp.cpu_count()))
            list_var_tuple =[var_tuple for i in range(simul)]
            results = p.map(simul_func, list_var_tuple)
        i = 0
        for r in results:
            rets['return_{}'.format(i)] = r
            i += 1
    else:
        logger_get_analytics.info("--- start simul for {}--- and  using 1 cpu".format(simul))
        for i in range(simul):
            rets['return_{}'.format(i)] = simul_func(var_tuple)

    ''' 
    for i in range(simul):
        contribution['last'] = temp_df.index[0]
        withdraw['last'] = temp_df.index[0]
        rebal['last'] = temp_df.index[0]
        df_full = simulate_data(df_full, asset_codes, forecasts, starting_price)
        df_full = df_full.apply(update_row, args=(dict_nb_share, contribution, withdraw, rebal, target_asset_codes_weight), axis=1)
        rets['return_{}'.format(i)] = df_full['portfolio_return']
    '''

    output = {}
    ret_5th = rets.cumsum().apply(lambda x: np.percentile(x, 5), axis=1)
    percentile_5th = pd.DataFrame({'percentile_5th': ret_5th, 'date': ret_5th.index.strftime('%m/%d/%Y')})
    output['percentile_5th'] = percentile_5th.to_dict(orient='records')
    ret_10th = rets.cumsum().apply(lambda x: np.percentile(x, 10), axis=1)
    percentile_10th = pd.DataFrame({'percentile_10th': ret_10th, 'date': ret_10th.index.strftime('%m/%d/%Y')})
    output['percentile_10th'] = percentile_10th.to_dict(orient='records')
    ret_25th = rets.cumsum().apply(lambda x: np.percentile(x, 25), axis=1)
    percentile_25th = pd.DataFrame({'percentile_25th': ret_25th, 'date': ret_25th.index.strftime('%m/%d/%Y')})
    output['percentile_25th'] = percentile_25th.to_dict(orient='records')
    ret_50th = rets.cumsum().apply(lambda x: np.percentile(x, 50), axis=1)
    percentile_50th = pd.DataFrame({'percentile_50th': ret_50th, 'date': ret_25th.index.strftime('%m/%d/%Y')})
    output['percentile_50th'] = percentile_50th.to_dict(orient='records')
    ret_75th = rets.cumsum().apply(lambda x: np.percentile(x, 75), axis=1)
    percentile_75th = pd.DataFrame({'percentile_75th': ret_75th, 'date': ret_75th.index.strftime('%m/%d/%Y')})
    output['percentile_75th'] = percentile_75th.to_dict(orient='records')
    ret_95th = rets.cumsum().apply(lambda x: np.percentile(x, 95), axis=1)
    percentile_95th = pd.DataFrame({'percentile_95th': ret_95th, 'date': ret_95th.index.strftime('%m/%d/%Y')})
    output['percentile_95th'] = percentile_95th.to_dict(orient='records')
    average_ret = rets.cumsum().apply(lambda x: np.mean(x), axis=1)
    average_port = pd.DataFrame({'average_port': average_ret, 'date': average_ret.index.strftime('%m/%d/%Y')})
    output['average_port'] = average_port.to_dict(orient='records')

    logger_get_analytics.info("monte carlo from {} to {} done".format(start_date, end_date))
    #logger_get_analytics.info(format(df_full))
    logger_get_analytics.info(format(rets))
    logger_get_analytics.info("--- %s seconds ---" % (time.time() - start_time))

    logger_get_analytics.info("percentile_5th {}".format(percentile_5th))
    from datetime import datetime



    rets['date'] = rets.index
    rets['date'] = rets['date'].dt.strftime('%m/%d/%Y')
    #output['returns'] = rets

    if ret == 'json':
        import json
        return json.loads(json.dumps(output))
    else:
        return rets


def back_test_portfolio(
                        initial_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        target_asset_codes_weight={}, #  dict asset_code:weight list in portfolio
                        invested_amount = 10000,
                        rebalancing_frequency='daily', # 'daily', 'weekly', 'yearly'
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
    df_h_p = get_prices(asset_codes=asset_codes, ret='df',
                                 start_date=datetime.datetime.combine(start_date, datetime.time.min),
                                 end_date=datetime.datetime.combine(end_date, datetime.time.min),
                                 )

    logger_get_analytics.info("Getting data --- %s seconds ---" % (time.time() - start_time))



    freq = get_freq(rebalancing_frequency, contribution, withdraw, 'monthly')

    print('df ={}, {}, {}'.format(df_h_p,set( df_h_p['code'].tolist()), freq))

    # ['{}_unbalanced_weight'.format(ac) for ac in asset_codes]
    df_full = None
    dict_nb_share = dict()
    for ac in asset_codes:
        data = df_h_p[df_h_p['code']==ac].copy().sort_values(by=['converted_date'])
        temp_df = pd.DataFrame(data=data['adjusted_close'].tolist(),
                               index=data['date'],
                               columns=['{}_adjusted_close'.format(ac)]
                               )
        temp_df['date'] = pd.to_datetime(temp_df.index)
        temp_df = temp_df.resample(freq, on='date').last()

        #temp_df['date'] = pd.to_datetime(temp_df.index)

        temp_df['{}_return'.format(ac)] = temp_df['{}_adjusted_close'.format(ac)].pct_change()
        temp_df.loc[temp_df.index[0], '{}_return'.format(ac)] = 0 # keeping the first row
        temp_df[['{}_weight'.format(ac), '{}_nb_shares'.format(ac), '{}_value'.format(ac) ]] = np.NaN
        dict_nb_share[ac] = (initial_asset_codes_weight[ac] * invested_amount) / temp_df.loc[temp_df.index[0], '{}_adjusted_close'.format(ac)]
        df_full = pd.DataFrame(index=temp_df.index) if df_full is None else df_full
        df_full = pd.merge(df_full, temp_df, left_index=True, right_index=True) #df_full.join(temp_df, how="outer")
        df_full = df_full[df_full['{}_return'.format(ac)].notna()]

    logger_get_analytics.info("--- end init %s seconds ---" % (time.time() - start_time))
    df_full['portfolio_value'] = 0
    df_full['portfolio_return'] = 0
    rebal = dict()
    contribution['last'] = temp_df.index[0]
    withdraw['last'] = temp_df.index[0]
    rebal['last'] = temp_df.index[0]
    rebal['freq'] = rebalancing_frequency

    df_full = df_full.apply(update_row, args=(dict_nb_share, contribution, withdraw, rebal, target_asset_codes_weight), axis=1)

    logger_get_analytics.info("back_test_portfolio from {} to {} done, columns {}".format(start_date, end_date, df_full.columns))
    logger_get_analytics.info(format(df_full))
    logger_get_analytics.info("--- %s seconds ---" % (time.time() - start_time))
    df_full = df_full.loc[:, ~df_full.columns.duplicated()]

    if 'date' in df_full.columns:
        df_full['date'] = pd.to_numeric(df_full['date'].dt.strftime('%Y%m%d'))

    if 'date_x' in df_full.columns:
        if 'date' not in df_full.columns:
            df_full['date'] = pd.to_numeric(df_full['date_x'].dt.strftime('%Y%m%d'))
        del df_full['date_x']
    if 'date_y' in df_full.columns:
        if 'date' not in df_full.columns:
            df_full['date'] = pd.to_numeric(df_full['date_y'].dt.strftime('%Y%m%d'))
        del df_full['date_y']


    df_full = df_full.replace(np.nan, 0)
    if ret == 'json':
        return df_full.to_json(orient='records')
    if ret == 'dict':
        return df_full.to_dict('records')
    else:
        return df_full


def efficient_frontier(
                        asset_codes=[], #  dict asset_code:weight list in portfolio
                        start_date=None,
                        end_date=None,
                        ret='json' # json, df
                        ):
    import pandas as pd
    import datetime
    import time
    start_time = time.time()

    start_date = (datetime.date.today() + datetime.timedelta(-350)) \
        if start_date is None else start_date
    end_date = (datetime.date.today() + datetime.timedelta(+1)) \
        if end_date is None else end_date

    from dateutil import rrule
    df_h_p = get_prices(asset_codes=asset_codes, ret='df',
                                 start_date=datetime.datetime.combine(start_date, datetime.time.min),
                                 end_date=datetime.datetime.combine(end_date, datetime.time.min),
                                 )
    date_l = rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date)
    df_full = pd.DataFrame(index=date_l)

    for ac in asset_codes:
        data = df_h_p[df_h_p['code']==ac].copy().sort_values(by=['converted_date'])
        temp_df = pd.DataFrame(data=data['adjusted_close'].tolist(),
                               index=data['date'],
                               columns=['{}_adjusted_close'.format(ac)]
                               )
        df_full = df_full.join(temp_df['{}_adjusted_close'.format(ac)], how="outer")
        df_full = df_full[df_full['{}_adjusted_close'.format(ac)].notna()]
    df_full.columns = [c.replace('_adjusted_close', '') for c in df_full.columns]
    from pypfopt import risk_models
    from pypfopt import expected_returns
    cov_m = risk_models.CovarianceShrinkage(df_full).ledoit_wolf()
    from pypfopt import EfficientFrontier
    mu = expected_returns.capm_return(df_full)
    ports = []
    for ret in [0.001, 0.002, 0.003, 0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.055, 0.06, 0.07, 0.08, 0.09, 0.1]:
        opt = EfficientFrontier(mu, cov_m)
        opt.efficient_return(ret)
        output = {}
        weights = opt.clean_weights()
        output['weights'] = weights
        output['expected_return'], output['volatility'], output['sharpe_ratio'] = opt.portfolio_performance()
        ports.append(output)

    logger_get_analytics.info("opto from {} to {} done".format(start_date, end_date))
    logger_get_analytics.info(format(ports))
    logger_get_analytics.info("--- %s seconds ---" % (time.time() - start_time))

    if ret == 'json':
        import json
        return json.dumps(ports)
    else:
        logger_get_analytics.info(format(ports))
        df_full = pd.DataFrame(ports, columns=['Code', 'weights'])
        return df_full

def portfolio_optimization(
                        asset_codes=[], #  dict asset_code:weight list in portfolio
                        optimisation_goal='max_sr', # max_sr, 'min_vol', 'opt'
                                            # 'min_vol_for_return',
                                            # 'min_vol_for_return',
                                            # 'max_rt_for_vol'
                        target_return = 0.02,
                        target_volatility = 0.02,
                        optimisation_type = 'mean_var', # 'mean_var', 'risk_parity', 'black_litterman'
                        viewdict = {}, # viewdict = { "BX4.PA":0.1, "CAC.PA":0.2, "500.PA":0.05 }
                        view_confidences = {}, # view_confidences = { "BX4.PA":0.1, "CAC.PA":0.2, "500.PA":0.05 }
                        asset_constraints = [{}], #  [{"code":"BX4.PA", "sign":"egt","value": 0.02},
                                                        # {"code":"CAC.PA", "sign":"elt","value": 0.06},
                                                    # {"code":"500.PA", "sign":"e","value": 0.10}, ]
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

    from dateutil import rrule

    df = get_universe(codes=",".join(asset_codes))

    df['full_code'] = df['Code'] + '.' + df['ExchangeCode']
    lstock = df['full_code'].tolist()

    lstockAssetType = df.to_dict('records')

    df_h_p = get_prices(asset_codes=lstock,start_date=start_date,end_date=end_date,
                             type="historical", ret_code=1, ret='df')


    date_l = rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date)
    df_full = pd.DataFrame(index=date_l)

    print('df_h_p={}'.format(df_h_p))

    for ac in asset_codes:
        data = df_h_p[df_h_p['code'] == ac].copy().sort_values(by=['converted_date'])
        temp_df = pd.DataFrame(data=data['adjusted_close'].tolist(),
                               index=pd.to_datetime(data['date']),
                               columns=['{}_adjusted_close'.format(ac)]
                               )
        # temp_df['{}_return'.format(ac)] = temp_df['{}_adjusted_close'.format(ac)].pct_change()
        # df_full = df_full.join(temp_df['{}_return'.format(ac)], how="outer")
        # df_full = df_full[df_full['{}_return'.format(ac)].notna()]
        #  print('temp_df={}, index = {}'.format(temp_df['{}_adjusted_close'.format(ac)], temp_df['{}_adjusted_close'.format(ac)].index.dtype))
        # print('df_full={}, index = {}'.format(df_full, df_full.index.dtype))

        df_full = df_full.join(temp_df['{}_adjusted_close'.format(ac)], how="outer")
        df_full = df_full[df_full['{}_adjusted_close'.format(ac)].notna()]

    df_full.columns = [c.replace('_adjusted_close', '') for c in df_full.columns]
    from pypfopt import risk_models
    from pypfopt import expected_returns
    df_full = df_full.dropna()
    #cov_m = risk_models.CovarianceShrinkage(df_full).ledoit_wolf()
    cov_m = risk_models.risk_matrix(df_full)
    from pypfopt import EfficientFrontier
    mu = expected_returns.capm_return(df_full)

    if optimisation_type == 'risk_parity':
        from pypfopt import expected_returns, HRPOpt
        rets = expected_returns.returns_from_prices(df_full)
        opt = HRPOpt(rets)
    elif optimisation_type == 'black_litterman':
        # You don't have to provide views on all the assets

        from pypfopt import BlackLittermanModel, objective_functions
        S = risk_models.CovarianceShrinkage(df_full).ledoit_wolf()
        bl = BlackLittermanModel(S, pi='equal', absolute_views=viewdict, view_confidences=view_confidences)
        S_bl = bl.bl_cov()
        ret_bl = bl.bl_returns()
        opt = EfficientFrontier(ret_bl, S_bl)
        opt.add_objective(objective_functions.L2_reg)
    if optimisation_type == 'max_diversification':
        from analytics.non_convex_opto import MaxDiversification
        logger_get_analytics.info(' max_diversification cov = {}'.format(cov_m))
        logger_get_analytics.info(' df_full  = {}'.format(df_full))
        cov_m = risk_models.risk_matrix(df_full)
        mu = expected_returns.ema_historical_return(df_full)
        opt = MaxDiversification(mu, cov_m)
    else:
        opt = EfficientFrontier(mu, cov_m)

    if bool(asset_constraints) and len(asset_constraints) > 0:
        for cons in asset_constraints:
            if 'code' in cons and cons['code'] in asset_codes:
                idx = opt.tickers.index('{}_adjusted_close'.format(cons['code']))
                if cons['sign'] == "egt":
                    opt.add_constraint(lambda w: w[idx] >= cons['value'])
                elif cons['sign'] == "elt":
                    opt.add_constraint(lambda w: w[idx] <= cons['value'])
                elif cons['sign'] == "e":
                    opt.add_constraint(lambda w: w[idx] == cons['value'])

    if optimisation_goal == 'min_vol':
        opt.min_volatility()
    elif optimisation_goal == 'max_sr':
        opt.max_sharpe()
    elif optimisation_goal == 'min_vol_for_return':
        opt.efficient_return(target_return)
    elif optimisation_goal == 'max_rt_for_vol':
        opt.efficient_risk(target_volatility)
    else:
        _make_output_weights =  opt.optimize()
        logger_get_analytics.info('_make_output_weights = {}'.format(_make_output_weights))

    output = {}
    weights = opt.clean_weights()
    output['weights'] = weights
    output['expected_return'], output['volatility'], output['sharpe_ratio'] = opt.portfolio_performance()
    output['asset_type_weights'] = {}
    print('lstockAssetType = {}'.format(lstockAssetType))

    output['asset_type_weights'] = get_asset_type_grouping(lstockAssetType, output, group_by='weights')
    ''' 
    for stock_data in lstockAssetType:
        if stock_data['AssetType'] not in output['asset_type_weights'].keys():
            output['asset_type_weights'][stock_data['AssetType']] = output['weights'][
                stock_data["Code"] + "." + stock_data["ExchangeCode"]]
        else:
            output['asset_type_weights'][stock_data['AssetType']] += output['weights'][
                stock_data["Code"] + "." + stock_data["ExchangeCode"]]
    '''

    #df_full = pd.DataFrame(weights.items(), columns=['Code', 'weights'])

    logger_get_analytics.info("opto from {} to {} done".format(start_date, end_date))
    logger_get_analytics.info(format(output))
    logger_get_analytics.info("--- %s seconds ---" % (time.time() - start_time))

    if ret == 'json':
        import json
        return json.dumps(output)
    elif ret == 'dict':
        return output
    else:
        logger_get_analytics.info(format(df_full))
        df_full = pd.DataFrame(weights.items(), columns=['Code', 'weights'])
        df_full['expected_return'] = output['expected_return']
        df_full['volatility'] = output['volatility']
        df_full['sharpe_ratio'] = output['sharpe_ratio']
        return df_full





if __name__ == '__main__':

    # get_asset_returns(asset_codes=["BX4.PA", "CAC.PA"])
    import pandas as pd
    import numpy as np
    import datetime
    start_date = (datetime.date.today() + datetime.timedelta(-3500))
    end_date = (datetime.date.today() + datetime.timedelta(1))

    '''
    
    jj = portfolio_optimization(
                                asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
                                optimisation_goal='max_sr',
                                start_date=start_date,
                                end_date=end_date,
                                ret='json')
    logger_get_analytics.info('max_sr = {}'.format(jj))

    jj = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_goal='min_vol',
        start_date=start_date,
        end_date=end_date,
        ret='json')
    logger_get_analytics.info('min_vol = {}'.format(jj))

    jj = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_goal='min_vol_for_return',
        target_return=0.03,
        start_date=start_date,
        end_date=end_date,
        ret='json')
    logger_get_analytics.info('min_vol_for_return = {}'.format(jj))

    jj = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_goal='max_rt_for_vol',
        target_volatility=0.04,
        start_date=start_date,
        end_date=end_date,
        ret='json')
    logger_get_analytics.info('max_rt_for_vol = {}'.format(jj))

    jj = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_type='risk_parity',
        optimisation_goal='opt',
        start_date=start_date,
        end_date=end_date,
        ret='json')
    logger_get_analytics.info('risk_parity = {}'.format(jj))

    jj = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_type='black_litterman',
        viewdict={"BX4.PA": 0.1, "CAC.PA": 0.3, "500.PA": -0.05, "AIR.PA": 0.05},
        view_confidences={"BX4.PA": 0.6, "CAC.PA": 0.8, "500.PA": 0.55, "AIR.PA": 0.85},
        start_date=start_date,
        end_date=end_date,
        ret='json')
    logger_get_analytics.info('black_litterman = {}'.format(jj))
    
    start_date = (datetime.date.today() + datetime.timedelta(-1700))
    end_date = (datetime.date.today() + datetime.timedelta(1))
    initial_asset_codes_weight = {"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1}




      # dict contribution amount : frequency ('monthly', 'yearly'),
    invested_amount = 10000
    rebalancing_frequency = 'monthly'
    df = monte_carlo_portfolio_simul(
        initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
        start_date=start_date,
        invested_amount=10000,
        end_date=end_date,
        nb_simul=1000,
        target_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
        contribution={'amount': 100, 'freq': 'monthly'},
        withdraw = {'amount': 100, 'freq': 'yearly'},
        multi_process=True,
        rebalancing_frequency=rebalancing_frequency,
        ret='json'
    )

    
    df = back_test_portfolio(
         initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
         target_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
         rebalancing_frequency = 'monthly',
         invested_amount=50,
         start_date=start_date,
         end_date=end_date,
         withdraw={'amount': 0, 'freq': 'yearly'},
         contribution={'amount': 50, 'freq': 'monthly'},
         ret='df'
    )


    rr = efficient_frontier(
            asset_codes= ['BX4.PA', 'CAC.PA', '500.PA', 'AIR.PA'],  # dict asset_code:weight list in portfolio
            start_date=start_date,
            end_date=end_date,
            ret='json'  # json, df
    )
'''
    # , "LVC.PA"
    import datetime as dat
    start_date = dat.date.today() + dat.timedelta(-200)
    end_date = dat.date.today() + dat.timedelta(1)
    start_date = dat.datetime.combine(start_date, dat.time.min)
    end_date = dat.datetime.combine(end_date, dat.time.min)

    jj = portfolio_optimization(
        asset_codes=["BX4.PA","OBLI.PA", "LQQ.PA", "STZ.PA","CAC.PA","SAN.PA"],
        optimisation_type='max_diversification',
        optimisation_goal='max_diversification',
        start_date=start_date,
        end_date=end_date,
        ret='dict')

    logger_get_analytics.info('max_diversification = {}, type {}'.format(jj,type(jj) ))
    result = back_test_portfolio(
        initial_asset_codes_weight=jj['weights'],
        target_asset_codes_weight=jj['weights'],
        rebalancing_frequency='monthly',
        invested_amount=50,
        start_date=start_date,
        end_date=end_date,
        withdraw={'amount': 0, 'freq': 'yearly'},
        contribution={'amount': 50, 'freq': 'monthly'},
        ret = 'json'
    )
    logger_get_analytics.info('result max_diversification backtest= {}'.format(result))

    #df.to_csv('/Users/sergengoube/PycharmProjects/investingclub/extract_{}.csv'.format(datetime.datetime.now()))