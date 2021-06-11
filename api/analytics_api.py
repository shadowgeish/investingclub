from flask import Flask
from flask_restful import Resource, Api
from flask_restful.reqparse import RequestParser
from flask_socketio import SocketIO, emit
from api.monte_carlo import MonteCarloSimulation
from api.SharePrices import LiveSharePrices


app = Flask(__name__)
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
api.add_resource(MonteCarloSimulation, '/simulation')
api.add_resource(LiveSharePrices, '/live_share_prices', resource_class_kwargs={'socket_io': socket_io})


if __name__ == '__main__':
    # app.run(debug=True, port=5001)
    socket_io.run(app, debug=True, port=5001)