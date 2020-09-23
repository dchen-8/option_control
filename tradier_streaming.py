import asyncio
import websockets
import mongo_client
import json
import requests
import os

from typing import Mapping

TRADIER_AUTH_TOKEN = os.environ.get('TRADIER_AUTH_TOKEN')

class TradierWebsocket:
    async def __aenter__(self):
        self._conn = websockets.connect('wss://ws.tradier.com/v1/markets/events')
        self.websocket = await self._conn.__aenter__()        
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._conn.__aexit__(*args, **kwargs)

    async def send(self, message):
        await self.websocket.send(message)

    async def receive(self):
        return await self.websocket.recv()

class TradierStreamingApi():

    def __init__(self):
        self.wss = TradierWebsocket()
        mongo_connect = mongo_client.Connect.get_connection()
        self.stocks_db = mongo_connect.stocks

        self._base_url = 'https://api.tradier.com/'
        self.headers  = {"Accept": "application/json",
                          "Authorization": "Bearer {}".format(TRADIER_AUTH_TOKEN)}

    def post(self, api_endpoint: str, params: Mapping[str, str] = {}):
        url = self._base_url + api_endpoint

        if not TRADIER_AUTH_TOKEN:
            raise ValueError('Missing TRADIER_AUTH_TOKEN')

        try:
            response = requests.post(url, headers=self.headers, params=params)
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

    def start_streaming(self):
        session_id = self.get_session_id()
        params = {
            'symbols': ['AAPL', 'TSLA'],
            'sessionid': session_id,
            'linebreak': True
        }
        payload = json.dumps(params)
        return asyncio.get_event_loop().run_until_complete(self.connect_and_consume(payload))

    def get_session_id(self):
        api_endpoint = '/v1/markets/events/session'
        params = {
            'Content-length': 0
        }
        response = self.post(api_endpoint, params)
        try:
            return response['stream']['sessionid']
        except ValueError as e:
            print(e)

    async def connect_and_consume(self, payload):
        async with self.wss as websocket:
            # payload = '{"symbols": ["SPY"], "sessionid": "SESSION_ID", "linebreak": true}'

            await websocket.send(payload)
            print(f"> {payload}")

            while True:
                try:
                    response = await websocket.receive()
                    response_dict = json.loads(response)
                    # print(response_dict)
                    # Write stream data to Mongo
                    self.stocks_db.streaming_data.insert_one(response_dict)
                except websockets.exceptions.ConnectionClosed as wss_connect_error:
                    print(wss_connect_error)
                    print('Websocket Disconnected. Wait 10 seconds and reconnect')
                    asyncio.sleep(10)
                    self.start_streaming()