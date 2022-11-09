import time
import pandas as pd
from datetime import datetime, timedelta

from icecream import ic
from concurrent.futures import ThreadPoolExecutor

from db_connector import PostgresConnector

class DataProcessor:
    '''DataProcessor class reads a data batch from csv files and inserts it to database.

    Attributes:
        db_connector:PostgresConnector
            database connector

        list_of_symbols_in_bars_1:str
            a list of symbols present in table bars_1
            this variable is used to decrease the number of database calls

        list_of_trades:str
            a list of trades to be inserted to the database

        list_of_errors:str
            a list of trades to be inserted to the database

        bars_1_df:pd.DataFrame
            pandas dataframe that contains data from table bars_1

    Methods:
        read_data(table_name:str, limit:int)
            reads data from table, number of rows is equal to limit

        check_if_symbol_in_bars_1(table_name:str, symbol:str)
            method returns True if symbol is in table bars_1

        create_list_of_symbols(table_name:str)
            makes a database query to recieve a list of unique symbols in bars_1,
            returns a list

        check_if_symbol_in_list(symbol:str)
            returns True if symbol in list_of_symbols_in_bars_1

        generate_error_message(trade:list, error_type:str)
            method recieves trade data and error type,
            generates error message and appends it to list_of_errors
            returns error message

        read_data_csv(filename:str)
            reads csv and returns pd.DataFrame

        create_df(data:list)
            recieves a list and returns pd.DataFrame

        check_minimum_price_over_n_days(symbol:str, candle_date:str, close:float, n_days:int)
            method checks if close price is bigger than symbol's minimum price
            over last 10 days, returns True if condition is met

        insert_trades()
            bulk insert trades to bars_1

        process_data_by_rows(trade:str)
            Method checks each row of data if it meets test case conditions.
            Appends a trade either to trades_list or errors_list.

        insert_errors()
            bulk insert errors to error_log

        delete_rows(table_name:str, limit:int)
            deletes processed rows, number of rows is equal to limit

        run(self, rows:int)
            runs the script, processes 20k rows, inserts errors and trades,
            deletes processed trades
    '''
    def __init__(self, config_filename:str, config_section:str):
        self.db_connector = PostgresConnector(config_filename, config_section)
        self.list_of_symbols_in_bars_1 = []
        self.list_of_trades = []
        self.list_of_errors = []

        self.bars_1_df = pd.DataFrame()

    def read_data(self, table_name:str='bars_2', limit:int = 20000) -> list:
        return self.db_connector.read_data_limit(table_name=table_name, limit=limit)

    def check_if_symbol_in_bars_1(self, table_name:str='bars_1', symbol:str=''):
        data = self.db_connector.read_data_by_symbol(table_name=table_name, symbol=symbol)
        if data:
            return True

    def create_list_of_symbols(self, table_name:str='bars_1'):
        data = self.db_connector.read_symbols_distinct(table_name=table_name)
        #list of tuples  -> list of symbols
        self.list_of_symbols_in_bars_1 = [x[0] for x in data]
        return [x[0] for x in data]

    def check_if_symbol_in_list(self, symbol:str):
        if symbol in self.list_of_symbols_in_bars_1:
            return True

    def generate_error_message(self, trade:list, error_type:str) -> list:
        '''error_types = symbol_error, price_error, no_data_error '''
        timestamp = time.time()
        trade_date = trade[1]
        symbol = trade[2]
        message = 'unknown error type'
        if error_type == 'symbol_error':
            message = "{} not present in tables_bars_1 on {}".format(symbol, trade_date)
        if error_type == 'price_error':
            message = '{} close price is not bigger than the minimum over the past 10 days on {}'.format(symbol, trade_date)
        if error_type == 'no_data_error':
            message = 'No values available in bars_2'
        self.list_of_errors.append([timestamp, trade_date, symbol, message])
        return [timestamp, trade_date, symbol, message]

    def read_data_csv(self, filename:str) -> pd.DataFrame:
        data = pd.read_csv(filename)
        return data

    def create_df(self, data:list) -> pd.DataFrame:
        df = pd.DataFrame(data)
        return df

    def check_minimum_price_over_n_days(self, symbol:str, candle_date:str, close:float, n_days:int=10) -> bool:
        self.bars_1_df['Date'] = pd.to_datetime(self.bars_1_df['Date'])
        candle_date = pd.to_datetime(candle_date)
        period = pd.Timedelta(n_days, "d")

        res = self.bars_1_df.loc[self.bars_1_df['Symbol'] == symbol]
        res2 = res.loc[(res['Date'] >= candle_date-period) & (res['Date'] <= candle_date)]
        if len(res2) > 0:
            minimum_price = res2['Close'].min()
            if close > minimum_price:
                return True
            else:
                return False

    def insert_trades(self):
        self.db_connector.insert_data_executemany(data=self.list_of_trades, table_name='bars_1')

    def process_data_by_rows(self, trade:str):
        time_spent = 0
        start_time = time.time()
        no_error = 0
        error = 0

        symbol = trade[2]
        candle_date = trade[1]
        close = trade[6]
        '''compare symbol to list of symbols in bars_1 '''
        if self.check_if_symbol_in_list(symbol):
            #continue some work

            if self.check_minimum_price_over_n_days(symbol, candle_date, close, n_days=10):
                #append data to bars_1

                no_error += 1
                self.list_of_trades.append(trade)

            else:
                #generate error message
                error += 1
                error_message = self.generate_error_message(trade, error_type='price_error')

        else:
            error += 1
            error_message = self.generate_error_message(trade, error_type='symbol_error')

        ic('processed ', len(data), 'rows. OK: ', no_error, ' ERROR: ', error)
        ic(time_spent)

    def insert_errors(self):
        self.db_connector.insert_error_data_list(data=self.list_of_errors, table_name='error_log')
        ic('errors inserted to db', len(self.list_of_errors))

    def delete_rows(self, table_name:str='bars_2', limit:int=20000):
        self.db_connector.delete_data(table_name='bars_2', limit=20000)

    def run(self, rows:int=20000):
        start_time = time.time()
        print('microservice started at ', start_time)
        #read bars_1 once at script start
        data = self.read_data_csv('bars_1_shuffled.csv')
        self.bars_1_df = self.create_df(data)

        #create once per run, append symbols to it if new symbol
        self.create_list_of_symbols(table_name='bars_1')
        #clean lists of trades and errors
        self.list_of_trades = []
        self.list_of_errors = []

        batch = self.read_data(table_name='bars_2', limit=rows)
        if len(batch) > 0:
            with ThreadPoolExecutor() as executor:
                executor.map(self.process_data_by_rows, batch)
        else:
            trade = [None, None, None]
            self.generate_error_message(trade, error_type='no_data_error')

        self.insert_errors()
        self.insert_trades()

        self.delete_rows(table_name='bars_2', limit=rows)

        ic(time.time() - start_time)


class TestDataProcessor:
    '''a class for manual testing DataProcessor '''
    def __init__(self, config_filename, config_section):
        self.data_processor = DataProcessor(config_filename, config_section)

    def test_read_data(self, table_name='bars_2', limit=20000):
        start_time = time.time()
        data = self.data_processor.db_connector.read_data_limit_20k(table_name='bars_2', limit=20000)
        time_spent = time.time() - start_time
        ic('data read in %s seconds' % (time_spent))
        ic('total records', len(data))
        ic('data first element', data[0])
        ic('data last element', data[-1])

    def check_if_symbol_in_bars_1(self, table_name='bars_1', symbol=''):
        start_time = time.time()
        result = self.data_processor.check_if_symbol_in_bars_1(table_name='bars_1', symbol=symbol)
        time_spent = time.time() - start_time
        ic(result)

    def test_create_list_of_symbols(self, table_name='bars_1'):
        start_time = time.time()
        result = self.data_processor.create_list_of_symbols(table_name='bars_1')
        time_spent = time.time() - start_time
        ic(len(self.data_processor.list_of_symbols_in_bars_1))
        ic(self.data_processor.list_of_symbols_in_bars_1)

    def test_common_elements(self):
        bars_1 = self.data_processor.create_list_of_symbols(table_name='bars_1')
        bars_2 = self.data_processor.create_list_of_symbols(table_name='bars_2')
        common_elements = [i for i in bars_1 if i in bars_2]
        ic('symbols in bars_1', len(bars_1))
        ic('symbols in bars_2', len(bars_2))
        ic('common_elements', len(common_elements))

    def test_run(self):
        self.data_processor.run(rows=20000)

    def run(self):
        #1 - read 20k rows
        #self.test_read_data(table_name='bars_2', limit=20000)
        #2 - read symbol from bars_1
        #self.check_if_symbol_in_bars_1(table_name='bars_1', symbol='ZXC')
        #3 - create list of symbols in bars_1
        #self.test_create_list_of_symbols(table_name='bars_1')

        #self.test_common_elements()

        self.test_run()



if __name__ == '__main__':
    test = TestDataProcessor(config_filename='database.ini', config_section='postgresql')
    test.run()
