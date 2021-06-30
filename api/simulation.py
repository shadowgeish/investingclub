from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from datetime import datetime
from analytics.analytics import monte_carlo_portfolio_simul, back_test_portfolio, portfolio_optimization

monte_carlo_request_parser = RequestParser(bundle_errors=False)


monte_carlo_request_parser.add_argument("asset_weights", type=dict, required=False,
                                        help="")

monte_carlo_request_parser.add_argument("start_date", type=str, required=False,
                                        help="Number of business days from the specified date to now")

monte_carlo_request_parser.add_argument("end_date", type=str, required=False,
                                        help="Number of business days from the specified date to now")

monte_carlo_request_parser.add_argument("InvestedAmount", type=int, required=False,
                                        help="Amount invested", default=10000)

monte_carlo_request_parser.add_argument("NbSimulation", type=int, required=False,
                                        help="Number of simulation", default=5)

monte_carlo_request_parser.add_argument("RebalancingFrequency", type=str, required=False,
                                        help="RebalancingFrequency")

monte_carlo_request_parser.add_argument("MultiPrecessorRun", type=int, required=False,
                                        help="MultiPrecessorRun")

monte_carlo_request_parser.add_argument("TargetAssetWeight", type=dict, required=False,
                                        help="TargetAssetWeight")

monte_carlo_request_parser.add_argument("RebalancyFrequency", type=str, required=False,
                                        help="RebalancyFrequency", default="monthly")

monte_carlo_request_parser.add_argument("Contribution", type=dict, required=False,
                                        help="Contribution")

monte_carlo_request_parser.add_argument("Withdraw", type=dict, required=False,
                                        help="withdraw")


class MonteCarloSimulation(Resource):

    def get(self):
        import datetime
        args = monte_carlo_request_parser.parse_args()

        start_date = (datetime.date.today() + datetime.timedelta(-3500))
        end_date = (datetime.date.today() + datetime.timedelta(1))
        invested_amount = args['InvestedAmount']
        rebalancing_frequency = args['RebalancyFrequency']
        nb_simul = args['NbSimulation']
        #["IWDA.LSE", "TDT.AS", "BX4.PA", "IAEX.AS", "VUSA.LSE", "STZ.PA", "LQQ.PA"]
        result = monte_carlo_portfolio_simul(
            initial_asset_codes_weight={"IWDA.LSE": 0.3, "BX4.PA": 0.2, "TDT.AS": 0.2, "IAEX.AS": 0.2, "STZ.PA": 0.1},
            start_date=start_date,
            invested_amount=invested_amount,
            end_date=end_date,
            nb_simul=nb_simul,
            target_asset_codes_weight={"IWDA.LSE": 0.3, "BX4.PA": 0.2, "TDT.AS": 0.2, "IAEX.AS": 0.2, "STZ.PA": 0.1},
            contribution={'amount': 100, 'freq': 'monthly'},
            withdraw={'amount': 100, 'freq': 'yearly'},
            multi_process=True,
            rebalancing_frequency=rebalancing_frequency,
            ret='json'
        )
        print('type = {}'.format(type(result)))
        return result, 200


aa_backtest_request_parser = RequestParser(bundle_errors=False)

aa_backtest_request_parser.add_argument("start_date", type=str, required=False,
                                        help="Number of business days from the specified date to now")

aa_backtest_request_parser.add_argument("end_date", type=str, required=False,
                                        help="Number of business days from the specified date to now")

aa_backtest_request_parser.add_argument("InvestedAmount", type=int, required=False,
                                        help="Amount invested", default=10000)

aa_backtest_request_parser.add_argument("RebalancingFrequency", type=str, required=False,
                                        help="RebalancingFrequency")

aa_backtest_request_parser.add_argument("TargetAssetWeight", type=dict, required=False,
                                        help="TargetAssetWeight")

aa_backtest_request_parser.add_argument("RebalancyFrequency", type=str, required=False,
                                        help="RebalancyFrequency", default="monthly")

aa_backtest_request_parser.add_argument("Contribution", type=dict, required=False,
                                        help="Contribution")

aa_backtest_request_parser.add_argument("Withdraw", type=dict, required=False,
                                        help="withdraw")


class AAbacktesting(Resource):

    def get(self):
        import datetime
        args = aa_backtest_request_parser.parse_args()

        start_date = (datetime.date.today() + datetime.timedelta(-3500))
        end_date = (datetime.date.today() + datetime.timedelta(1))
        invested_amount = args['InvestedAmount']
        rebalancing_frequency = args['RebalancyFrequency']
        nb_simul = args['NbSimulation']

        result = back_test_portfolio(
            initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
            target_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
            rebalancing_frequency='monthly',
            invested_amount=50,
            start_date=start_date,
            end_date=end_date,
            withdraw={'amount': 0, 'freq': 'yearly'},
            contribution={'amount': 50, 'freq': 'monthly'},
            ret='json'
        )
        return result, 200


class MeanVarOptimization(Resource):

    def get(self):
        import datetime
        args = aa_backtest_request_parser.parse_args()

        start_date = (datetime.date.today() + datetime.timedelta(-3500))
        end_date = (datetime.date.today() + datetime.timedelta(1))
        invested_amount = args['InvestedAmount']
        rebalancing_frequency = args['RebalancyFrequency']
        nb_simul = args['NbSimulation']

        result = portfolio_optimization(
            asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
            optimisation_goal='min_vol_for_return',
            target_return=0.03,
            start_date=start_date,
            end_date=end_date,
            ret='json')

        return result, 200


class MaxDiversification(Resource):

    def get(self):
        import datetime
        args = aa_backtest_request_parser.parse_args()
        return optimization(args), 200


def optimization(args):

    # asset_codes, optimisation_goal, start_date = None, end_date = None,
    # rebalancing_frequency = 'monthly', target_return = 0.03, invested_amount = 10000,
    # nb_simul = 1000

    start_date = (datetime.date.today() + datetime.timedelta(-3500))
    end_date = (datetime.date.today() + datetime.timedelta(1))

    start_date =  (datetime.date.today() + datetime.timedelta(-3500)) if start_date is None else start_date
    end_date = (datetime.date.today() + datetime.timedelta(1)) if start_date is None else start_date

    result = portfolio_optimization(
        asset_codes=["BX4.PA", "CAC.PA", "500.PA", "AIR.PA"],
        optimisation_goal='min_vol_for_return',
        target_return=0.03,
        start_date=start_date,
        end_date=end_date,
        ret='json')

    return result