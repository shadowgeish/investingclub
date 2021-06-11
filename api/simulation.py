from datetime import datetime
from analytics.analytics import monte_carlo_portfolio_simul

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))


# Create a handler for our read (GET) people
def montecarlo():
    """
    This function responds to a request for /api/analytics/montecarlo
    with the complete lists of return over the period

    :return:        table array of portfolio returns
    """

    import datetime
    start_date = (datetime.date.today() + datetime.timedelta(-3500))
    end_date = (datetime.date.today() + datetime.timedelta(1))
    invested_amount = 10000
    rebalancing_frequency = 'monthly'
    df = monte_carlo_portfolio_simul(
        initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
        start_date=start_date,
        invested_amount=invested_amount,
        end_date=end_date,
        nb_simul=10,
        target_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
        contribution={'amount': 100, 'freq': 'monthly'},
        withdraw={'amount': 100, 'freq': 'yearly'},
        multi_process=True,
        rebalancing_frequency=rebalancing_frequency,
        ret='json'
    )
    return df