


def test_monte_carlo_api():

    import requests, uuid, json
    # Add your subscription key and endpoint
    endpoint = "http://localhost:5000/api/v1/montecarlo"

    # Add your location, also known as region. The default is global.
    # This is required if using a Cognitive Services resource.

    params = {
        'api-version': '3.0',
        'from': 'en'
    }
    constructed_url = endpoint

    # You can pass more than one object in body.
    body = [{
        'asset_codes_weight':
            {"IWDA.LSE": 0.3, "BX4.PA": 0.2, "TDT.AS": 0.2, "IAEX.AS": 0.2, "STZ.PA": 0.1}
    }]

    request = requests.get(constructed_url, params=params, json=body)
    print('request = {}'.format(request.text))
    response = request.json()
    print('response = {}'.format(response))


if __name__ == '__main__':
    #load_stock_universe()
    test_monte_carlo_api()
    #load_exchange_list()
