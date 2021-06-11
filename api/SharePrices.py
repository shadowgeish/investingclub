from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from datetime import datetime
from analytics.analytics import get_historical_data
from asset_prices.historical_prices import get_share_live_prices


def live_stock_prices():

    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        self.socket_io.sleep(60)
        count += 1
        self.socket_io.emit('my_response',
                      {'data': 'Server generated event', 'count': count})

    return result, 200
