import pandas as pd
import unittest
import time

from events import Event, InfoEvent, TradeEvent, ErrorEvent

class DataStream:
    '''this class takes parquet data as input
    checks all conditions described in test case 1.2
    and generates trade by trade stream of events

    Attributes:
        data: pd.DataFrame
            sample data from parquet file
        last_timestamp: str
            timestamp of the last trade sent by server to clients

    Methods:
        to_json(data:pd.DataFrame)
            using built-in pandas func, converts pandas dataframe to json

        select_stacked_timestamps(row:pd.DataFrame)
            selects all trade with the same timestamp
            to send them simultaneously

        check_timestamp_order(row:pd.DataFrame)
            a func to ensure correct time order of trades,
            returns True if trade's timestamp is bigger then last_trade's timestamp

        run(n_element:int)
            a procedure to perform all required actions on data
            recieves the number of element to read data from dataframe
            produces TradeEvent to send it to websocket client
            produces ErrorEvent after all data is sent
    '''
    def __init__(self, filename:str='trades_sample.parquet'):
        self.data = pd.read_parquet(filename)
        '''last_timestamp is required to ensure timestamp order '''
        self.last_timestamp = self.data['timestamp'].iloc[0]

    def to_json(self, data:pd.DataFrame):
        out = data.to_json(orient='records')[1:-1]
        if len(data) == 1:
            return (out)
        else:
            return out

    def select_stacked_timestamps(self, row:pd.DataFrame):
        '''a func to select ALL trades with same timestamp
        to ensure simultanious send'''
        df = self.data[self.data['timestamp'] == row.timestamp]
        return df

    def check_timestamp_order(self, row:pd.DataFrame) -> bool:
        '''1 dont send trades with timestamp older than last sent timestamp
        2 dont send trades with current timestamp, as they are already
        stacked and sent '''
        if row.timestamp.iloc[0] > self.last_timestamp:
            return True

    async def run(self, n_element:int) -> Event:
        try:
            row = self.data.iloc[n_element]
            row = self.select_stacked_timestamps(row)
            if self.check_timestamp_order(row):
                trades_data = self.to_json(row)
                self.last_timestamp = row.timestamp.iloc[0]
                return TradeEvent(trades_data)
            else:
                pass

        except IndexError as e:
            return ErrorEvent('Data stream finished, no more rows in data.')

class TestDataStream(unittest.TestCase):
    '''some basic unit tests for DataStream class '''

    def setUp(self):
        self.datastream = DataStream()

    def test_to_json(self):
        data = pd.DataFrame([1, '2', 3, 'asd'])
        a = self.datastream.to_json(data)
        b = data.to_json(orient='records')[1:-1]
        self.assertEqual(a, b)

    def test_check_timestamp_order(self):
        row = pd.DataFrame([[time.time(), 1, 2, '3']], columns=['timestamp', 'a', 'b', 'c'])
        self.datastream.last_timestamp = time.time() - 123
        self.assertTrue(self.datastream.check_timestamp_order(row))

if __name__ == '__main__':
    unittest.main()
