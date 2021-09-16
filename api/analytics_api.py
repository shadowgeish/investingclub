from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from flask_socketio import SocketIO, emit
from api.simulation import MonteCarloSimulation, AAbacktesting, MeanVarOptimization, MaxDiversification
from api.stocks import StockUniverse, StockData, StockPrices, IntradayStockPrices, PushIntradayStockPrices, \
    StockUniverseIntradayData, PushBulkIntradayStockPrices
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)

### swagger specific ###
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "InvestingClub Analytics API"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
### end swagger specific ##

api = Api(app, prefix="/api/v1")

# Set this variable to "threading", "eventlet" or "gevent" to test the
# different async modes, or leave it set to None for the application to choose
# the best option based on installed packages.
async_mode = None
# socket_io = SocketIO(app, async_mode=async_mode)
socket_io = SocketIO(app, async_mode=async_mode)

users = [
    {"email": "masnun@gmail.com", "name": "Masnun", "id": 1}
]


def get_user_by_id(user_id):
    for x in users:
        if x.get("id") == int(user_id):
            return x


subscriber_request_parser = RequestParser(bundle_errors=True)
subscriber_request_parser.add_argument("name", type=str, required=True, help="Name has to be valid string")
subscriber_request_parser.add_argument("email", required=True)
subscriber_request_parser.add_argument("id", type=int, required=True, help="Please enter valid integer as ID")


class SubscriberCollection(Resource):
    def get(self):
        return users

    def post(self):
        args = subscriber_request_parser.parse_args()
        users.append(args)
        return {"msg": "Subscriber added", "subscriber_data": args}


class Subscriber(Resource):
    def get(self, id):
        user = get_user_by_id(id)
        if not user:
            return {"error": "User not found"}

        return user

    def put(self, id):
        args = subscriber_request_parser.parse_args()
        user = get_user_by_id(id)
        if user:
            users.remove(user)
            users.append(args)

        return args

    def delete(self, id):
        user = get_user_by_id(id)
        if user:
            users.remove(user)

        return {"message": "Deleted"}


api.add_resource(SubscriberCollection, '/subscribers')
api.add_resource(Subscriber, '/subscribers/<int:id>')
api.add_resource(MonteCarloSimulation, '/montecarlo')
api.add_resource(AAbacktesting, '/aabacktesting')
api.add_resource(MaxDiversification, '/maximum_diversificaton')
api.add_resource(MeanVarOptimization, '/mean_var_opt')
#api.add_resource(MaxDiversification, '/realestate') # real estate prices
#api.add_resource(MonteCarloSimulation, '/lifeinsurance') # life insurance
api.add_resource(StockUniverse, '/stock_universe') # country, type, name - OK
api.add_resource(StockUniverseIntradayData, '/stock_universe_intraday_data/<string:codes>') # country, type, name - OK
api.add_resource(StockData, '/stock_data/<string:code>')
api.add_resource(StockPrices, '/stock_prices/<string:code>')
api.add_resource(PushIntradayStockPrices, '/load_intraday_stock_prices/<string:code>')
api.add_resource(PushBulkIntradayStockPrices, '/load_bulk_intraday_stock_prices')
api.add_resource(IntradayStockPrices, '/intraday_stock_prices/<string:code>')
# name, exchange, description, asset class, esg ratings, financial data
# ETF => composition, issuer logo, AUM,


if __name__ == '__main__':
    # app.run(debug=True, port=5001)
    socket_io.run(app, debug=True , host='0.0.0.0', port=5001)