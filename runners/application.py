from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from flask_socketio import SocketIO, emit
from flask_cors import CORS, cross_origin
from api.simulation import MonteCarloSimulation, AAbacktesting, MeanVarOptimization, MaxDiversification
from api.stocks import StockUniverse, StockData, StockPricesOld, \
    StockPrices, PushBulkIntradayStockPrices, StockDataAndPrices, HelloWord, PortfolioAnalytics
from flask_swagger_ui import get_swaggerui_blueprint
from threading import Timer

application = app = Flask(__name__)
CORS(app, support_credentials=True)
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
#async_mode = None
# socket_io = SocketIO(app, async_mode=async_mode)
#socket_io = SocketIO(app, async_mode=async_mode)
from flask_socketio import SocketIO, emit, disconnect
socket_ = SocketIO(app, async_mode=None)

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


import pytz
from marketsimulator.orderbook import Orderbook
from datetime import datetime
OrderBookList = dict()
OrderUI = dict()
tz = pytz.timezone('Europe/Paris')

order_book_request_parser = RequestParser(bundle_errors=False)
order_book_request_parser.add_argument("quantity", type=float, required=False,
                                         help=" order code", default=0)
order_book_request_parser.add_argument("price", type=float, required=False,
                                         help="order price", default=0)
order_book_request_parser.add_argument("is_buy", type=int, required=False,
                                         help="order side, 1 = buy, 0 = sell", default=-1)
order_book_request_parser.add_argument("order_id", type=int, required=False,
                                         help="order id", default=-1)
order_book_request_parser.add_argument("client_order_uid", type=str, required=False,
                                         help="client order uid", default="")


class AddOrder(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)

    def post(self, code):
        # initialize an empty orderbook book for Santander shares (san)
        args = order_book_request_parser.parse_args()
        qty = args['quantity']
        price = args['price']
        side = args['is_buy']
        client_order_uid = args['client_order_uid']

        ob = OrderBookList[code] if code in OrderBookList.keys() else Orderbook(ticker=code)
        OrderBookList[code] = ob

        if side not in [0, 1] or price <= 0 or qty <= 0:
            return {"result": "wrong parameters", "order" : {}}, 200

        paris_now = datetime.now(tz)
        paris_now_date = datetime.strptime(paris_now.strftime("%d%m%Y0855%z"), '%d%m%Y%H%M%z')

        OrderUI[code] = OrderUI[code] + 1 if code in OrderUI.keys() else 1
        is_buy = True if side == 1 else False
        neword, trade_list = ob.send(uid=OrderUI[code], is_buy=is_buy, qty=qty, price=price, timestamp=paris_now,
                                     client_order_uid=client_order_uid)
        print(neword)
        print(ob)
        socket_.emit('new_orders', neword.order_data())
        socket_.emit('new_trades', trade_list)
        socket_.emit('best_bid_ask', ob.best_bid_ask)

        return {"result": "Order inserted", "order": neword.order_data(),
                "trade_list": trade_list}, 200


class GetOrder(Resource):
    # df['CustomRating'] = df.apply(lambda x: custom_rating(x['Genre'], x['Rating']), axis=1)

    def get(self, code):
        # initialize an empty orderbook book for Santander shares (san)
        args = order_book_request_parser.parse_args()
        order_id = args['order_id']
        ob = OrderBookList[code] if code in OrderBookList.keys() else Orderbook(ticker=code)
        OrderBookList[code] = ob

        order = ob.get_order(order_id)
        order_data = {} if order is None else order.order_data()
        return {"result": "Order inserted", "order": order_data}, 200


class GetBBO(Resource):

    def get(self, code):
        # initialize an empty orderbook book for Santander shares (san)
        full_code_list = code.split(',')
        bbos = []
        for cod in full_code_list:
            ob = OrderBookList[code] if cod in OrderBookList.keys() else Orderbook(ticker=code)
            bbos.append({"code": ob.best_bid_ask})

        return {"best_bid_ask": bbos}, 200


class GetOrderBook(Resource):

    def get(self, code):
        # initialize an empty orderbook book for Santander shares (san)
        ob = OrderBookList[code] if code in OrderBookList.keys() else Orderbook(ticker=code)
        ob_df = ob.top_bis_asks(10)
        return {"order_book": ob_df.to_dict(orient='records')}, 200


api.add_resource(SubscriberCollection, '/subscribers')
api.add_resource(Subscriber, '/subscribers/<int:id>')
api.add_resource(MonteCarloSimulation, '/montecarlo')
api.add_resource(AAbacktesting, '/aabacktesting')
api.add_resource(MaxDiversification, '/maximum_diversificaton')
api.add_resource(MeanVarOptimization, '/mean_var_opt')
#api.add_resource(MaxDiversification, '/realestate') # real estate prices
#api.add_resource(MonteCarloSimulation, '/lifeinsurance') # life insurance
api.add_resource(StockUniverse, '/stock_universe') # country, type, name - OK
api.add_resource(AddOrder, '/add_order/<string:code>') # country, type, name - OK
api.add_resource(GetOrder, '/get_order/<string:code>') # country, type, name - OK
api.add_resource(GetBBO, '/get_bbo/<string:code>') # country, type, name - OK
api.add_resource(GetOrderBook, '/get_order_book/<string:code>') # country, type, name - OK
api.add_resource(HelloWord, '/') # country, type, name - OK
api.add_resource(StockPrices, '/stock_prices/<string:codes>') # country, type, name - OK
api.add_resource(StockDataAndPrices, '/stock_latest_prices/<string:codes>') # country, type, name - OK Add filtering and sorting
api.add_resource(StockData, '/stock_data/<string:code>') #- Ok Add ESG data ok, Add mobile version (light)
api.add_resource(StockPricesOld, '/stock_prices_old/<string:code>') # - to delete
api.add_resource(PushBulkIntradayStockPrices, '/load_bulk_intraday_stock_prices')
api.add_resource(PortfolioAnalytics, '/compute_portfolio_analytics')
# name, exchange, description, asset class, esg ratings, financial data
# ETF => composition, issuer logo, AUM,


class RepeatedTimer(object):
    def __init__(self, interval, function):#, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        #self.args       = args
        #self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def run(self):
        self.is_running = False
        self.start()
        self.function()#*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self.run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def get_stock_prices():
    from datetime import datetime
    from asset_prices.referencial import get_universe, get_indx_cc_fx_universe
    import json
    import pytz
    tz = pytz.timezone('Europe/Paris')
    from pymongo import MongoClient
    print('Getting price data')
    server_run = 'https://stocks.investingclub.io' #'https://stocks.investingclub.io'  # http://localhost:5005
    # server_run = 'localhost'  # localhost
    collection_name = "real_time_prices"
    db_name = "asset_analytics"
    access_db = "mongodb+srv://sngoube:Yqy8kMYRWX76oiiP@cluster0.jaxrk.mongodb.net/asset_analytics?retryWrites=true&w=majority"
    server = MongoClient(access_db)
    from aiohttp import ClientSession
    # session = ClientSession()
    import requests
    session = requests.Session()

    paris_now = datetime.now(tz)
    dtt = paris_now
    dtt_s = datetime.strptime(paris_now.strftime("%d%m%Y0855%z"), '%d%m%Y%H%M%z')
    dtt_e = datetime.strptime(paris_now.strftime("%d%m%Y2130%z"), '%d%m%Y%H%M%z')
    ''' 
    if dtt_s >= dtt or dtt >= dtt_e or dtt.weekday() > 4:
        print('No need to run {}, {}, {}'.format(dtt_s, dtt, dtt_e))
        return
    '''
    ddf = get_universe()
    ddf2 = get_indx_cc_fx_universe()
    ddf = ddf.append(ddf2)
    ddf['full_code'] = ddf['Code'] + '.' + ddf['ExchangeCode']
    lstock = ddf['full_code'].tolist()  # ddf.to_dict(orient='records')

    sublists = [lstock[x:x + 20] for x in range(0, len(lstock), 20)]
    stringlist = []

    for subset in sublists:
        stringlist.append(','.join(subset))

    for sublist in stringlist:
        list_closing_prices = []

        print('Getting data for sub string {}'.format(sublist))
        sreq = "https://eodhistoricaldata.com/api/real-time/CAC.PA?api_token=60241295a5b4c3.00921778&fmt=json&s={}"

        with session.get(sreq.format(sublist)) as response:
            #data = response.read()
            data = response.text
            # list_closing_prices = await response.json(content_type=None)
        try:
            #print('Loading {}'.format(data))
            list_closing_prices = json.loads(data)

        except ValueError as e:
            list_closing_prices = []
            print('Error loading data {} '.format(data))

        if len(list_closing_prices) > 0:
            from asset_prices.prices import update_bulk_prices
            update_bulk_prices(prices=list_closing_prices, type='real_time')

            # Loading data
            #sreq = "{}/api/v1/load_bulk_intraday_stock_prices"
            #str_req = sreq.format(server_run)

            p#rint('Loading {}'.format(str_req))
            # logger_rtapi.info('list_closing_prices {}'.format(list_closing_prices))
            #with session.post(str_req, json=list_closing_prices) as response:
                #data = response.read()
            #    data = response.text
            #try:
            #    stock_prices = json.loads(data)
            #    print('Seding intraday_prices {}'.format(stock_prices))
            #except ValueError as e:
            #    print('Error loading data {} '.format(data))


if __name__ == '__main__':
    rt = RepeatedTimer(9, get_stock_prices)
    socket_.run(app, debug=True, host='0.0.0.0', port=5005)