import csv
import time
import pandas as pd

import psycopg2
from configparser import ConfigParser
from icecream import ic

from data_shuffle import ShuffleData

class DBConnector:
    '''Basic class DBConnector.
    Should implement methods:
        config()
            reads config file
        execute()
            executes a db query
    '''
    def __init__(self):
        pass

    def config(self):
        '''reads database config from file '''
        pass

    def execute(self):
        '''executes a database query '''
        pass

class PostgresConnector(DBConnector):
    '''PostgreSQL database connector

    Attributes:

        config_filename:str
            config file location

        config_section:str
            config file section name with postgresql settings

        connection_parameters:dict
            postgres database connection parameters

    Methods:

        config()
            reads postgres config file and returns database connection parameters

        format_data(data:list)
            reads OHLCV from file and returns a list:
            Date,Symbol,Adj Close,Close,High,Low,Open,Volume
            ->
            date, symbol, open, high, low, close, adj_close, volume

        check_null_data(row:list)
            checks if ALL data is present in OHLCV row and returns True
            method is used to skip missing/broken data

        preprocess_data(source_filename:str)
            read data from file, changes columns order and filters null values
            returns a list of preprocessed bars

        execute(command:str, data=None, fetch=bool, executemany=bool)
            Executes a query command.
            Data contains some data like data to be inserted to database.
            If fetch is True, method returns result of a query with cur.fetchall().
            If executemany is True, trades/errors are inserted into the database
            with a single query for better execution speed.

        insert_data_executemany(data:list, table_name:str, fetch:bool)
            bulk insert trades to database

        read_data_limit(table_name:str, limit:int)
            reads n rows from database, amount of rows is based on limit parameter

        read_data_by_symbol(table_name:str, symbol:str)
            reads all data from table for a specific symbol

        read_symbols_distinct(table_name:str)
            creates a list of all symbols present in table

        insert_error_data_list(data:list, table_name:str)
            bulk insert errors to database

        delete_data(table_name:str, limit:int)
            deletes n rows from table

        find_minimum_price(symbol:str, start_date:str, candle_date:str)
            returns all data for a specific symbol
            with timestamps between start_date and candle_date

        delete_tables()
            deletes all tables from database

        create_tables()
            creates tables bars_1, bars_2 and error_log
    '''
    def __init__(self, config_filename:str, config_section:str):
        self.config_filename = config_filename
        self.config_section = config_section

        self.connection_parameters = self.config()

    def config(self) -> dict:
        parser = ConfigParser()
        parser.read(self.config_filename)
        db = {}
        if parser.has_section(self.config_section):
            params = parser.items(self.config_section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(self.config_section, self.config_filename))
        return db

    def format_data(self, data: list) -> list:
        '''
        Date,Symbol,Adj Close,Close,High,Low,Open,Volume
        ->
        date, symbol, open, high, low, close, adj_close, volume
        '''
        return [data[0], data[1], data[6], data[4], data[5], data[3], data[2], data[7]]

    def check_null_data(self, row: list) -> bool:
        if all(element for element in row):
            return True
        else:
            return False

    def preprocess_data(self, source_filename:str) -> list:
        start_time = time.time()
        result = []
        with open(source_filename, 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip the header row.
            for row in reader:
                row = self.format_data(row)
                if self.check_null_data(row):
                    result.append(row)
        time_spent = time.time() - start_time
        ic("data preprocessed in %s seconds" % (time_spent))
        return result

    def execute(self, command:str, data=None, fetch=False, executemany=False):
        '''executes a database query '''
        start_time = time.time()
        conn = None
        result = None
        try:
            conn = psycopg2.connect(**self.connection_parameters)
            cur = conn.cursor()
            if fetch:
                cur.execute(command)
                result = cur.fetchall()
            else:
                if executemany:
                    cur.executemany(command, data)
                else:
                    cur.execute(command, data)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            time_spent = time.time() - start_time
            ic("executed in %s seconds" % (time_spent))
            if conn is not None:
                conn.close()
            return result

    def insert_data_executemany(self, data:list, table_name:str, fetch=False):
        q = "INSERT INTO {} (candle_date, symbol, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)
        self.execute(q, data, fetch=False, executemany=True)

    def read_data_limit(self, table_name:str='bars_1', limit:int=0):
        q = "select * from {} limit {}".format(table_name, limit)
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

    def read_data_by_symbol(self, table_name:str='bars_1', symbol:str='ABC'):
        q = "select * from {} where symbol = '{}'".format(table_name, symbol)
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

    def read_symbols_distinct(self, table_name:str='bars_1') -> list:
        '''returns a list of symbols present in table '''

        q = "select distinct symbol from {}".format(table_name)
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

    def insert_error_data_list(self, data:list, table_name:str='error_log'):
        q = "INSERT INTO {} (launch_timestamp, date, symbol, message) VALUES (%s, %s, %s, %s)".format(table_name)
        self.execute(q, data, fetch=False, executemany=True)

    def delete_data(self, table_name:str, limit:int):
        q = """DELETE from {} WHERE id IN (SELECT id FROM {} LIMIT {})
            """.format(table_name, table_name, limit)
        self.execute(q, data=None, fetch=False, executemany=False)

    def find_minimum_price(self, symbol:str, start_date:str, candle_date:str):
        q = """SELECT * from bars_1 WHERE symbol = '{}' AND candle_date >= '{}' AND candle_date <= '{}'
            """.format(symbol, start_date, candle_date)
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

    def delete_tables(self):
        q = """ DROP SCHEMA public CASCADE; CREATE SCHEMA public; """
        self.execute(q, data=None, fetch=False, executemany=False)

    def create_tables(self):
        q = """
        CREATE TABLE bars_1 (
            id BIGSERIAL PRIMARY KEY,
            candle_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            open FLOAT NOT NULL,
            high FLOAT NOT NULL,
            low FLOAT NOT NULL,
            close FLOAT NOT NULL,
            adj_close FLOAT NOT NULL,
            volume FLOAT NOT NULL
        );
        CREATE TABLE bars_2 (
            id BIGSERIAL PRIMARY KEY,
            candle_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            open FLOAT NOT NULL,
            high FLOAT NOT NULL,
            low FLOAT NOT NULL,
            close FLOAT NOT NULL,
            adj_close FLOAT NOT NULL,
            volume FLOAT NOT NULL
        );
        CREATE TABLE error_log (
            id BIGSERIAL PRIMARY KEY,
            launch_timestamp FLOAT(4),
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
        self.execute(q, data=None, fetch=False, executemany=False)

'''a class for manual testing PostgreSQL database connector '''
class TestPostgressConnector:
    def __init__(self, config_filename, config_section):
        self.db_connector = PostgresConnector(config_filename, config_section)
        self.shuffle = ShuffleData()

    def test_delete_tables(self):
        self.db_connector.delete_tables()

    def test_create_tables(self):
        self.db_connector.create_tables()

    def test_preprocess_data(self, source_filename='bars_1.csv'):
        data = self.db_connector.preprocess_data(source_filename)
        ic(data)

    def test_insert_data(self, source_filename='bars_1.csv', table_name='bars_1'):
        data = self.db_connector.preprocess_data(source_filename)
        self.db_connector.insert_data(data, table_name)

    def test_insert_data_executemany(self, source_filename='bars_1.csv', table_name='bars_1'):
        data = self.db_connector.preprocess_data(source_filename)
        self.db_connector.insert_data_executemany(data, table_name)

    def test_read_symbols_distinct(self, table_name):
        data = self.db_connector.read_symbols_distinct(table_name)
        ic(len(data))
        ic(data)

    def test_delete_data(self, table_name, limit):
        self.db_connector.delete_data(table_name, limit)

    #read_data_limit_20k(self, table_name='bars_1', limit=20000):
    def test_read_data_limit(self, table_name='error_log', limit = 99):
        data = self.db_connector.read_data_limit(table_name, limit)
        ic('len of data tuple', len(data))
        if len(data) > 0:
            ic('first element:', data[0])
            ic('last element:', data[-1])
            ic(type(data[-1][1]))

    def test_read_data_by_symbol(self, table_name='bars_1', symbol='MMM'):
        data = self.db_connector.read_data_by_symbol(table_name=table_name, symbol=symbol)
        ic('len of data tuple', len(data))
        if len(data) > 0:
            ic('first element:', data[0])
            ic('last element:', data[-1])
            ic(type(data[-1][1]))

    def test_find_minimum_price(self, symbol, start_date, candle_date):
        data = self.db_connector.find_minimum_price(symbol, start_date, candle_date)
        ic('len of data tuple', len(data))
        if len(data) > 0:
            ic(data)
            ic('first element:', data[0])
            ic('last element:', data[-1])
            ic(type(data[-1][1]))



    def run(self):
        #self.test_delete_tables()
        #self.test_create_tables()
        #self.shuffle.run()
        #self.test_preprocess_data(source_filename='bars_1.csv')
        #self.test_insert_data_executemany(source_filename='bars_1_shuffled.csv', table_name='bars_1')
        #self.test_insert_data_executemany(source_filename='bars_2_shuffled.csv', table_name='bars_2')
        self.test_read_data_limit(table_name='bars_1', limit = 200)
        #self.test_read_data_by_symbol(table_name='bars_1', symbol='MMM')

        #self.test_read_symbols_distinct(table_name='bars_1')

        #self.test_delete_data(table_name='error_log', limit = 99)

        #self.test_find_minimum_price(symbol='MMM', start_date='2010-02-01', candle_date='2019-11-11')
        pass



if __name__ == '__main__':
    test = TestPostgressConnector(config_filename='database.ini', config_section='postgresql')
    test.run()
