from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from datetime import datetime
from analytics.analytics import monte_carlo_portfolio_simul

monte_carlo_request_parser = RequestParser(bundle_errors=False)

monte_carlo_request_parser.add_argument("DaysBack", type=int, required=False,
                                        help="Number of business days from the specified date to now")

monte_carlo_request_parser.add_argument("DaysFwd", type=int, required=False,
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


class StockPrice(Resource):

    def get(self):
        import datetime
        args = monte_carlo_request_parser.parse_args()

        start_date = (datetime.date.today() + datetime.timedelta(-3500))
        end_date = (datetime.date.today() + datetime.timedelta(1))
        invested_amount = args['InvestedAmount']
        rebalancing_frequency = args['RebalancyFrequency']
        nb_simul = args['NbSimulation']
        result = monte_carlo_portfolio_simul(
            initial_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
            start_date=start_date,
            invested_amount=invested_amount,
            end_date=end_date,
            nb_simul=nb_simul,
            target_asset_codes_weight={"BX4.PA": 0.3, "CAC.PA": 0.4, "500.PA": 0.2, "AIR.PA": 0.1},
            contribution={'amount': 100, 'freq': 'monthly'},
            withdraw={'amount': 100, 'freq': 'yearly'},
            multi_process=True,
            rebalancing_frequency=rebalancing_frequency,
            ret='json'
        )
        return result, 200