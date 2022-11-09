import time

import asyncio
import websockets
import json

from events import Event, InfoEvent, TradeEvent, ErrorEvent
from datastream import DataStream

class WSServer:
    '''Websocket server with a single channel - 'sample'.
    Sends data from parquet file to all connected clients.

    Attributes:
        host:str
            websocket server host

        port:int
            websocket server port

        connected_clients:set
            contains all clients connected to websocket

        available_channels:set
            contains a list of all channels available for clients

        data_stream:DataStream
            a separate module that reads data from parquet and
            produces events trade by trade to send them to clients

        delay:float
            delays send_message func, for testing purposes

    Methods:
        build_message(event:Event)
            takes data from Event and serializes it to json string

        handler(websocket, path)
            handler method manages connected clients,
            and sends them data stream if they choose correct channel

        data_stream_logic(websocket)
            contains a loop that calls data_stream on each iteration
            to recieve events with trade data or error data
            sends events to clients

        send_message(websocket, message:str)
            sends json to all connected clients

        run()
            runs asyncio loop with websocket server
    '''
    def __init__(self, host:str='localhost', port:int=8000, delay:float = 0, filename:str='trades_sample.parquet'):
        self.host = host
        self.port = port
        self.connected_clients = set()
        self.available_channels = set(['sample'])
        self.data_stream = DataStream(filename=filename)
        self.delay = delay

    '''builds a message and covnerts it to json '''
    def build_message(self, event:Event) -> str:
        message = {'type': event.type, 'timestamp': event.timestamp, 'data': event.data}
        return json.dumps(message)

    async def handler(self, websocket, path):
        message = await websocket.recv()
        while True:
            if message in self.available_channels:
                message = self.build_message(InfoEvent('You are now connected to {} channel.'.format(message)))
                await websocket.send(message)
                # Register.
                self.connected_clients.add(websocket)
                try:
                    await self.data_stream_logic(websocket)

                finally:
                    # Unregister.
                    self.connected_clients.remove(websocket)

            else:
                await websocket.send("""Wrong channel. Available channels: {}""".format(self.available_channels))
                await asyncio.sleep(10)

    async def data_stream_logic(self, websocket):
        n_element = 0
        while True:
            event = await self.data_stream.run(n_element)
            n_element += 1

            if event:
                message = self.build_message(event)
                await self.send_message(websocket, message)

                if event.type == 'ErrorEvent':
                    break

    async def send_message(self, websocket, message):
        try:
            for client in self.connected_clients:
                await websocket.send(message)

        except websockets.ConnectionClosedOK:
            pass

        await asyncio.sleep(self.delay)

    def run(self):
        start_server = websockets.serve(self.handler, self.host, self.port)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    server = WSServer(host='localhost', port=8000, delay=0, filename='trades_sample.parquet')
    server.run()
