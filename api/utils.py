
import pandas as _pd
import numpy as _np

def get_date_from_str_or_default(datestr, default_date_obj):
    import datetime
    try:
        date_obj = datetime.datetime.strptime(datestr, '%Y%m%d')
    except:
        import sys
        print("Oops!", sys.exc_info()[0], "occurred with string date =", datestr)
        date_obj = default_date_obj
    return date_obj


def get_asset_type_grouping(stock_list, stock_values, group_by='weights'):
    asset_type_values = {}

    if group_by not in stock_values.keys():
        return asset_type_values

    for stock_data in stock_list:
        key = stock_data["Code"] + "." + stock_data["ExchangeCode"]
        if key in stock_values[group_by].keys():
            if stock_data['AssetType'] not in asset_type_values.keys():
                asset_type_values[stock_data['AssetType']] = 0
            asset_type_values[stock_data['AssetType']] += stock_values[group_by][key]

    return asset_type_values


def get_asset_type_grouping_weights(asset_type_values, total_value):
    asset_type_weights = {}
    for asset_type in asset_type_values.keys():
        asset_type_weights[asset_type] = asset_type_values[asset_type] / total_value
    return asset_type_weights


def get_returns(values):
    returns = values.pct_change()
    monthly_returns = values.resample('M').ffill().pct_change()
    return returns, monthly_returns

def min_max_dates(list_of_date_dict_stock_weight):
    import datetime
    first_date = datetime.datetime.fromisoformat( list_of_date_dict_stock_weight[0]['date']).date()
    min_date = first_date
    max_date = first_date
    for holding in list_of_date_dict_stock_weight:
        dat = datetime.datetime.fromisoformat(holding['date']).date()
        print('dat = {}'.format(dat))
        if dat > max_date:
            max_date = dat
        if dat < min_date:
            min_date = dat
    return min_date, max_date


def stock_list(list_of_date_dict_stock_weight):
    stocks = []
    for holding_data in list_of_date_dict_stock_weight:
        stocks.append(list(holding_data['holdings'].keys()))
    stocks = [item for sublist in stocks for item in sublist]
    return list(set(stocks))


def stock_total_value(list_of_date_dict_stock_weight, stock_code_prices, stock_data_list, max_date):
    import datetime
    import pandas as pd
    # portfolio_historical_values, portfolio_stock_values, portfolio_stock_weights, total_value
    portfolio_historical_values = []
    portfolio_stock_values = [] # [{date, dict stock value},{}]
    portfolio_stock_weights = [] # [{date, dict stock value},{}]
    last_portfolio_stock_values = {}
    for holding_data in list_of_date_dict_stock_weight:
        fdat = datetime.datetime.fromisoformat(holding_data['date']).date()
        dat = fdat.strftime("%Y%m%d")
        stock_code_prices['date'] = pd.to_datetime(stock_code_prices['date'])
        stock_code_prices.index = stock_code_prices['date']
        #print('stock_code_prices === {}, df.dtypes = {}, max_date ={}'.format(stock_code_prices, stock_code_prices.dtypes, max_date))

        sub_date_df = stock_code_prices.loc[dat] #stock_code_prices[stock_code_prices['date']==dat].copy()
        #print('sub_date_df = {}, for dat {}'.format(sub_date_df, dat))
        full_stock_data = pd.merge(sub_date_df, stock_data_list, on="code")
        #print('full_stock_data = {}'.format(full_stock_data))
        holdings = holding_data['holdings']
        dat_portfolio_stock_values = {'date': fdat.strftime("%Y-%m-%d")}
        dat_portfolio_values = {'date': fdat.strftime("%Y-%m-%d")}
        for stock_code in holdings.keys():
            sub_holding_df = sub_date_df[sub_date_df['code'] == stock_code]
            #print('sub_holding_df = {} for stock_code {}'.format(sub_holding_df, stock_code))
            stock_data = sub_holding_df.to_dict('records')
            #print('stock_data = {}'.format(stock_data))
            val = stock_data[0]['adjusted_close'] * holdings[stock_code]
            #print('val = {}'.format(val))
            if 'holdings' not in dat_portfolio_stock_values.keys():
                dat_portfolio_stock_values['holdings'] = {}
            dat_portfolio_stock_values['holdings'][stock_code] = val
            if 'value' not in dat_portfolio_values.keys():
                dat_portfolio_values['value'] = 0
            dat_portfolio_values['value'] += val
        dat_portfolio_stock_values['portfolio_value'] = dat_portfolio_values['value']
        portfolio_stock_values.append(dat_portfolio_stock_values)
        portfolio_historical_values.append(dat_portfolio_values)
        if max_date == fdat:
            last_portfolio_stock_values = dat_portfolio_stock_values

    for holdings_data in portfolio_stock_values:
        dat_portfolio_stock_weights = {'date': fdat.strftime("%Y-%m-%d")}
        for stock_code in holdings_data['holdings'].keys():
            if 'weights' not in dat_portfolio_stock_weights.keys():
                dat_portfolio_stock_weights['weights'] = {}
            dat_portfolio_stock_weights['weights'][stock_code] = holdings_data['holdings'][stock_code]/holdings_data['portfolio_value']
        portfolio_stock_weights.append(dat_portfolio_stock_weights)

    return portfolio_historical_values, portfolio_stock_values, portfolio_stock_weights, last_portfolio_stock_values


def volatility(returns, periods=252, annualize=True):
    """Calculates the volatility of returns for a period"""

    std = returns.std()
    if annualize:
        return std * _np.sqrt(periods)
    return std


def greeks(returns, benchmark_returns, periods=252.):
    """Calculates alpha and beta of the portfolio"""
    from scipy import stats
    ret = [item for sublist in returns.values for item in sublist]
    b_ret = [item for sublist in benchmark_returns.values for item in sublist]
    slope, intercept, r_value, p_value, std_err = stats.linregress(ret, b_ret)
    return slope, intercept


def sharpe(returns, rf=0., periods=252, annualize=True, smart=False):
    """
    Calculates the sharpe ratio of access returns
    If rf is non-zero, you must specify periods.
    In this case, rf is assumed to be expressed in yearly (annualized) terms
    Args:
        * returns (Series, DataFrame): Input return series
        * rf (float): Risk-free rate expressed as a yearly (annualized) return
        * periods (int): Freq. of returns (252/365 for daily, 12 for monthly)
        * annualize: return annualize sharpe?
        * smart: return smart sharpe ratio
    """
    if rf != 0 and periods is None:
        raise Exception('Must provide periods if rf != 0')

    divisor = returns.std(ddof=1)

    res = returns.mean() / divisor

    if annualize:
        return res * _np.sqrt(
            1 if periods is None else periods)

    return res


def max_drawdown(values):
    """Calculates the maximum drawdown"""
    return (values / values.expanding(min_periods=0).max()).min() - 1


def last_holding_date(portfolio_stock_values,  max_date, min_date):
    from datetime import datetime

    t_1_date = datetime.strptime(min_date, '%Y-%m-%d')
    t_date = datetime.strptime(max_date, '%Y-%m-%d')
    for holding_data in portfolio_stock_values:

        current_date = datetime.strptime(holding_data['date'], '%Y-%m-%d')
        if current_date > t_1_date and current_date != t_date:
            t_1_date = datetime.strptime(holding_data['date'], '%Y-%m-%d')

    return t_1_date


def winners_loosers(stock_prices_list, t_date, t_1_date):
    from datetime import datetime
    winners = []
    loosers = []
    t_h = {}
    t_1_h = {}
    t_date = datetime.strptime(t_date, '%Y-%m-%d')
    for stock_price in stock_prices_list:
        stock_data = {"stock":stock_price['code'], "price":stock_price['adjusted_close']}
        current_date = datetime.strptime(stock_price['date'], '%Y-%m-%d')
        # print("current_date = {}, t_date = {}, t_1_date = {}, stock_price = {}".format(current_date, t_date, t_1_date, stock_price))
        if current_date == t_date:
            t_h[stock_price['code']] = stock_data #stock_price['close']
        elif current_date == t_1_date:
            t_1_h[stock_price['code']] = stock_data

    for stock in t_h.keys():
        stock_data = {"stock":stock, "price":t_h[stock]['price'], "variation":((t_h[stock]['price']-t_1_h[stock]['price'])/t_1_h[stock]['price'])}
        if t_1_h[stock]['price'] <= t_h[stock]['price']:
            winners.append(stock_data)
        else:
            loosers.append(stock_data)
    return winners, loosers