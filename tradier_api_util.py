import os
import datetime
import requests

from collections import defaultdict
from typing import Mapping, List, Union

TRADIER_AUTH_TOKEN = os.environ.get('TRADIER_AUTH_TOKEN')


class Tradier:
    def __init__(self):
        self._base_url = 'https://api.tradier.com/'
        self.headers  = {"Accept": "application/json",
                          "Authorization": "Bearer {}".format(TRADIER_AUTH_TOKEN)}
 
    def request(self, api_endpoint: str, params: Mapping[str, str] = {}):
        url = self._base_url + api_endpoint

        if not TRADIER_AUTH_TOKEN:
            raise ValueError('Missing TRADIER_AUTH_TOKEN')

        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json()
            return None
        except requests.ConnectionError as e:
            print(e)
        except requests.HTTPError as e:
            print(e)
        except requests.RequestException as e:
            print(e)
        return None

    def get_option_chain_data(self, symbol, expiration):
        api_endpoint = '/v1/markets/options/chains'
        params = {
            'symbol': symbol,
            'expiration': expiration
        }
        response = self.request(api_endpoint, params) 
        print(len(response.get('options').get('option')))
        return response

    def get_option_expiration(self, symbol):
        api_endpoint = '/v1/markets/options/expirations'
        params = {
            'symbol': symbol,
        }
        response = self.request(api_endpoint, params)
        if not response:
            return None
        dates = response['expirations']['date']
        results = []
        for date in dates:
            values = {
                'measurement': 'options_expiration',
                'tags': {
                    'dates': date,
                    'symbol': symbol
                },
                'fields': {
                    'value': 1
                }
            }
            results.append(values)
        return results

    def get_three_months_historical_stocks(self, stock):
        today = datetime.datetime.now()
        three_months_ago = today - datetime.timedelta(days=30)
        params = {
            'symbol': stock,
            'interval': 'daily',
            'start': three_months_ago.strftime('%Y-%m-%d'),
            'end': today.strftime('%Y-%m-%d')
        }
        api_endpoint = '/v1/markets/history'
        response = self.request(api_endpoint, params)
        if response is None or not response:
            return None
        return response.get('history').get('day')

    def parse_historical_stocks(self, data, symbol):
        if data is None or not data:
            pass
        historical_data = data.get('history').get('day')
        results = []
        for day in historical_data:
            result = defaultdict(dict)

            # Measurements
            result['measurement'] = 'historical_stocks'

            # Tags
            result['tags']['date'] = day.get('date')
            result['tags']['symbol'] = symbol

            # Fields
            result['fields']['open'] = day.get('open')
            result['fields']['high'] = day.get('high')
            result['fields']['low'] = day.get('low')
            result['fields']['close'] = day.get('close')
            result['fields']['volume'] = day.get('volume')
        
            results.append(result)
        return results

    def get_clock(self):
        api_endpoint = '/v1/markets/clock'
        response = self.request(api_endpoint)
        return response.get('clock')

    def get_calendar(self, month: str, year: str):
        api_endpoint = '/v1/markets/calendar'
        params = {'month': month, 'year': year}
        response = self.request(api_endpoint, params)
        # results = self._calendar_to_list(response)
        return response

    def _calendar_to_list(self, data):
        results = []
        if data is None or not data:
            pass
        calendar_dates = data.get('calendar').get('days').get('day')
        for row in calendar_dates:
            result = defaultdict(dict)

            # Measurement
            result['measurement'] = 'calendar_dates'

            # Tags
            result['tags']['date'] = row.get('date')
            status = row.get('status')
            result['tags']['status'] = status
            result['tags']['description'] = row.get('description')
            premarket = row.get('premarket')
            if premarket:
                result['tags']['premarket_start'] = premarket.get('start')
                result['tags']['premarket_end'] = premarket.get('end')
            open_time = row.get('open')
            if open_time:
                result['tags']['open_start'] = open_time.get('start')
                result['tags']['open_end'] = open_time.get('end')
            postmarket = row.get('postmarket')
            if postmarket:
                result['tags']['postmarket_start'] = postmarket.get('start')
                result['tags']['postmarket_end'] = postmarket.get('end')

            # Fields
            result['fields']['market_status'] = 1 if status == 'True' else 0

            # Append
            results.append(result)
        return results

    def get_symbol(self, symbol):
        api_endpoint = '/v1/markets/quotes'
        params = {'symbols': symbol}
        response = self.request(api_endpoint, params)
        result = self.symbol_to_list(response)
        return result

    def symbol_to_list(
        self, data: Mapping[str, Union[str, int]]) -> List[Mapping[str, Union[str, int]]]:
        try:
            results = data.get('quotes').get('quote')
        except AttributeError as e:
            print(e)
            results = []
        
        # Check if results is a list, if not: change into List
        if not isinstance(results, List):
            return [results]
        return results
