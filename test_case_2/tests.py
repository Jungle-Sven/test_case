import unittest

from websocket_client import WSClient

class TestWSClient(unittest.TestCase):
    '''some basic unit tests for WSClient class '''

    def setUp(self):
        self.client = WSClient(host='localhost', port=8000, channel='sample')

    def test_build_url(self, host='localhost', port=8000):
        a = self.client.build_url(host=host, port=port)
        b = 'ws://localhost:8000'
        self.assertEqual(a, b)

if __name__ == '__main__':
    unittest.main()
