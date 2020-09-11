# /usr/local/bin/python3

from collections import defaultdict
from influxdb import InfluxDBClient
from typing import List, Mapping

client = InfluxDBClient('192.168.86.99', 8086, 'root', 'root', 'mydb')

class OptionControlInfluxDB():

    def __init__(self, host='192.168.86.99', port=8086, username='root', password='root', database='stocks'):
        self._client = InfluxDBClient(host, port, username, password, database)

    def switch_database(self, db_name):
        self._client.switch_database(db_name)

    def write(self, data_to_write: List[Mapping[str, str]], db_name = '') -> None:
        if db_name:
            self.switch_database(db_name)
        self._client.write_points(data_to_write)
    
    def get_option_expirations(self):
        self.switch_database('options')
        response = self._client.query('SELECT * FROM options_expiration')
        results = defaultdict(list)
        for each in response.get_points():
            results[each['symbol']].append(each['dates'])
        return results