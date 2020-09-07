import os
import datetime
import requests

from collections import defaultdict
from typing import Mapping, List, Union

TRADIER_AUTH_TOKEN = os.environ.get('TRADIER_AUTH_TOKEN')

STOCKS_MAPPING = {
    'time': [],
    'tags': ['symbol', 'description', 'exch', 'type', 'bidexch', 
             'bid_date', 'askexch', 'ask_date', 'root_symbols',
             'trade_date'],
    'fields': ['last', 'change', 'volume', 'open', 'high', 
               'low', 'close', 'bid', 'ask', 'change_percentage', 
               'average_volume', 'last_volume', 'prev_close', 'week_52_high',
               'week_52_low', 'bidsize', 'asksize'],
}

class Tradier:
    def __init__(self):
        self._base_url = 'https://sandbox.tradier.com'
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
        except requests.RequestException as e:
            return e

    def get_three_months_historical_stocks(self, stock):
        today = datetime.datetime.now()
        three_months_ago = today - datetime.timedelta(days=30)
        params = {
            'symbol': stock,
            'interval': 'daily',
            'start': three_months_ago.strftime('%Y-%m-%d'),
            'end': today.strftime('%Y-%m-%d')
        }
        print(params)
        api_endpoint = '/v1/markets/history'
        response = self.request(api_endpoint, params)
        print(response)
        return self.parse_historical_stocks(response, stock)

    def parse_historical_stocks(self, data, symbol):
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
        results = self._calendar_to_list(response)
        return results

    def _calendar_to_list(self, data):
        results = []
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

    def get_symbol(self, symbol, return_for_influx = False):
        api_endpoint = '/v1/markets/quotes'
        params = {'symbols': symbol}
        response = self.request(api_endpoint, params)
        result = self.symbol_to_list(response)
        if return_for_influx:
            return self.parse_to_mapping(result)
        return result

    def symbol_to_list(
        self, data: Mapping[str, Union[str, int]]) -> List[Mapping[str, Union[str, int]]]:
        results = data.get('quotes').get('quote')
        if not isinstance(results, List):
            return [results]
        return results

    def parse_to_mapping(self, data: List[Mapping[str, Union[str, int]]]):
        mapping_used = STOCKS_MAPPING
        time_map = mapping_used.get('time')
        tags_map = mapping_used.get('tags')
        fields_map = mapping_used.get('fields')

        results = []
        for row in data:
            result = defaultdict(dict)
            result['measurement'] = 'stocks'
            for key, value in row.items():
                if key in time_map:
                    result['time'][key] = value
                if key in tags_map:
                    result['tags'][key] = value
                if key in fields_map:
                    result['fields'][key] = value
            results.append(result)
        
        return results