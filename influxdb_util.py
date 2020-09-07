# /usr/local/bin/python3

from influxdb import InfluxDBClient
from typing import List, Mapping

client = InfluxDBClient('192.168.86.99', 8086, 'root', 'root', 'mydb')

class OptionControlInfluxDB():

    def __init__(self, host='192.168.86.99', port=8086, username='root', password='root', database='stocks'):
        # super().__init__()
        # host = ''
        # port = 8086
        # username = 'root'
        # password = 'root'
        # database = 'options'
        self._client = InfluxDBClient(host, port, username, password, database)

    def switch_database(self, db_name):
        self._client.switch_database(db_name)

    def write(self, data_to_write: List[Mapping[str, str]], db_name = '') -> None:
        if db_name:
            self.switch_database(db_name)
        self._client.write_points(data_to_write)

# result = client.query('select * from mydb;')
# result = client.get_list_measurements()

# print("Result: {0}".format(result))