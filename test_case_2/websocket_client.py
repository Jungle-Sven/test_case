import asyncio
import websockets
import json

from icecream import ic

class WSClient:
    '''websocket client to test websocket server
    can connect to 1 channel - 'sample'

    Attributes:

        host:str
            websocket server host

        port:int
            websocket server port

        channel:str
            name of channel to connect to

    Methods:

        build_url(host:str, port:str)
            returns a string containing websocket address to connect to

        connect(url:str, channel:str)
            connects to websocket server and sends a message with desired channel name

        read_response(response:str)
            deserializes json to a dict and prints it

        run()
            starts asyncio loop with websocket connection
    '''
    def __init__(self, host:str, port:str, channel:str):
        self.host = host
        self.port = port
        self.channel = channel

    def build_url(self, host:str, port:str) -> str:
        return 'ws://{}:{}'.format(host, port)

    async def connect(self, url:str, channel:str):
        async with websockets.connect(url) as websocket:
            await websocket.send(channel)
            while True:
                response = await websocket.recv()
                self.read_response(response)

    def read_response(self, response:str) -> dict:
        response = json.loads(response)
        ic(response)
        return response

    def run(self):
        url = self.build_url(self.host, self.port)
        asyncio.get_event_loop().run_until_complete(self.connect(url, self.channel))

if __name__ == '__main__':
    client = WSClient(host='localhost', port=8000, channel='sample')
    client.run()
