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

aa_backtest_request_parser.add_argument("target_return", type=float, required=False,
                                        help="target_return")

aa_backtest_request_parser.add_argument("target_risk", type=float, required=False,
                                        help="target_risk")


class AAbacktesting(Resource):

    def get(self):
        import datetime
        args = aa_backtest_request_parser.parse_args()

        start_date = (datetime.date.today() + datetime.timedelta(-3500))
        end_date = (datetime.date.today() + datetime.timedelta(1))
        invested_amount = args.get('InvestedAmount', 10000) #['InvestedAmount']
        rebalancing_frequency = args.get('RebalancyFrequency', 'monthly') #args['RebalancyFrequency']
        #nb_simul =  args.get('NbSimulation', '100')

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


class MaxDiversificationPortfolio(Resource):

    def post(self):
        from flask import request
        import datetime as dat
        from api.utils import get_date_from_str_or_default
        args = aa_backtest_request_parser.parse_args()

        start_date = dat.date.today() + dat.timedelta(-200)
        end_date = dat.date.today() + dat.timedelta(1)
        start_date = dat.datetime.combine(start_date, dat.time.min)
        end_date = dat.datetime.combine(end_date, dat.time.min)

        opto_settings = request.get_json()
        print('opto_settings = {}'.format(opto_settings))
        asset_codes_initial_weight = opto_settings['assetCodesInitialWeight']
        optimization_type = opto_settings['optomizationType']
        optimisation_goal = opto_settings['optomisationGoal']
        amount_invested = opto_settings['amountInvested']
        asset_codes = list(asset_codes_initial_weight.keys())
        print('asset_codes = {}'.format(asset_codes))

        target_return = opto_settings['targetReturn']
        target_volatility = opto_settings['targetVolatility']
        optimization_constraints = opto_settings['contraintesOptomization']
        optimization_constraints = []
        for constraints in optimization_constraints:
            optimization_constraints.append(
                {"code": constraints['code'], "sign": constraints['sign'], "value": constraints['value']})
        # [{"code": "BX4.PA", "sign": "egt", "value": 0.02},
        # {"code":"CAC.PA", "sign":"elt","value": 0.06},
        # {"code":"500.PA", "sign":"e","value": 0.10}]

        # start_date = get_date_from_str_or_default(None,
        #                                          (dat.date.today() + dat.timedelta(-200)))
        # end_date = get_date_from_str_or_default(None,
        #                                        (dat.date.today() + dat.timedelta(1)))

        jj = portfolio_optimization(
            asset_codes=asset_codes,  # ["OBLI.PA", "LQQ.PA", "STZ.PA", "CAC.PA", "SAN.PA", "AETH.PA", "ABTC.PA"],
            optimisation_type=optimization_type,
            optimisation_goal=optimisation_goal,
            target_return=target_return,
            target_volatility=target_volatility,
            start_date=start_date,
            end_date=end_date,
            ret='dict')

        # print('max_diversification = {}, type {}'.format(jj, type(jj)))
        result = back_test_portfolio(
            initial_asset_codes_weight=jj['weights'],
            target_asset_codes_weight=jj['weights'],
            rebalancing_frequency='weekly',
            invested_amount=amount_invested,
            start_date=start_date,
            end_date=end_date,
            withdraw={'amount': 0, 'freq': 'yearly'},
            contribution={'amount': 0, 'freq': 'monthly'},
            ret='dict'
        )
        # print('result max_diversification backtest= {}'.format(result))
        full_result = {'Opto': jj, 'Backtest': result}
        return full_result, 200


class MaxDiversification(Resource):

    def post(self):
        import datetime
        from flask import request
        import datetime as dat
        from api.utils import get_date_from_str_or_default
        args = aa_backtest_request_parser.parse_args()

        start_date = dat.date.today() + dat.timedelta(-200)
        end_date = dat.date.today() + dat.timedelta(1)
        start_date = dat.datetime.combine(start_date, dat.time.min)
        end_date = dat.datetime.combine(end_date, dat.time.min)

        opto_settings = request.get_json()
        print('opto_settings = {}'.format(opto_settings))
        asset_codes_initial_weight = opto_settings['assetCodesInitialWeight']
        optimization_type = opto_settings['optomizationType']
        optimisation_goal = opto_settings['optomisationGoal']
        amount_invested = opto_settings['amountInvested']
        asset_codes = list(asset_codes_initial_weight.keys())
        print('asset_codes = {}'.format(asset_codes))

        target_return = opto_settings['targetReturn']
        target_volatility = opto_settings['targetVolatility']
        optomization_contraints = opto_settings['contraintesOptomization']
        opto_contraints = []
        for contraints in optomization_contraints:
            opto_contraints.append({"code": contraints['code'], "sign": contraints['sign'], "value": contraints['value']})
        #[{"code": "BX4.PA", "sign": "egt", "value": 0.02},
        # {"code":"CAC.PA", "sign":"elt","value": 0.06},
        # {"code":"500.PA", "sign":"e","value": 0.10}]

        #start_date = get_date_from_str_or_default(None,
        #                                          (dat.date.today() + dat.timedelta(-200)))
        #end_date = get_date_from_str_or_default(None,
        #                                        (dat.date.today() + dat.timedelta(1)))

        jj = portfolio_optimization(
            asset_codes=asset_codes, # ["OBLI.PA", "LQQ.PA", "STZ.PA", "CAC.PA", "SAN.PA", "AETH.PA", "ABTC.PA"],
            optimisation_type= optimization_type,
            optimisation_goal= optimisation_goal,
            target_return=target_return,
            target_volatility=target_volatility,
            start_date=start_date,
            end_date=end_date,
            ret='dict')
        #print('max_diversification = {}, type {}'.format(jj, type(jj)))
        result = back_test_portfolio(
            initial_asset_codes_weight=jj['weights'],
            target_asset_codes_weight=jj['weights'],
            rebalancing_frequency='weekly',
            invested_amount=amount_invested,
            start_date=start_date,
            end_date=end_date,
            withdraw={'amount': 0, 'freq': 'yearly'},
            contribution={'amount': 0, 'freq': 'monthly'},
            ret='dict'
        )
        #print('result max_diversification backtest= {}'.format(result))
        full_result = {'Opto':jj, 'Backtest': result}
        return full_result, 200


def optimization(args):

    # asset_codes, optimisation_goal, start_date = None, end_date = None,
    # rebalancing_frequency = 'monthly', target_return = 0.03, invested_amount = 10000,
    # nb_simul = 1000
    import datetime
    start_date = (datetime.date.today() + datetime.timedelta(-3500))
    end_date = (datetime.date.today() + datetime.timedelta(1))

    result = portfolio_optimization(
        asset_codes = ["IWDA.LSE", "TDT.AS", "BX4.PA", "IAEX.AS", "VUSA.LSE", "STZ.PA", "LQQ.PA"],
        optimisation_type='max_diversification',
        optimisation_goal='max_diversification',
        start_date=start_date,
        end_date=end_date,
        ret='json')

    return result, 200