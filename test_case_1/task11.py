import datetime

from icecream import ic

from db_connector import PostgresConnector

class Task11:
    def __init__(self, config_filename, config_section):
        self.db_connector = PostgresConnector(config_filename, config_section)

    def query_1(self, table_name='bars_1'):
        q1 = '''
        WITH req_acl AS (
        SELECT symbol, adj_close, lag(adj_close, 40) OVER (ORDER BY id) as adj_close_lag
        FROM bars_1
        WHERE candle_date >= '2019-01-01' AND candle_date <= '2019-12-31'
        )

        SELECT distinct symbol
        FROM req_acl
        WHERE adj_close > adj_close_lag
        '''

        q2 = '''
        SELECT distinct symbol
        FROM bars_1
        '''
        return q1, q2


    def question_1(self, table_name='bars_1'):
        command1, command2 = self.query_1(table_name=table_name)
        data = self.db_connector.execute(command=command1, data=None, fetch=True, executemany=False)
        data2 = self.db_connector.execute(command=command2, data=None, fetch=True, executemany=False)
        result = len(data) / len(data2)
        ic(len(data))
        ic(data[0:20])
        ic(data[-1])
        ic('% symbols from bars_1 for which condition is satisfied: ', result)
        pass

    def query_2(self, table_name='bars_1'):
        q2 = '''
        SELECT AVG(adj_close * volume) FROM bars_1
        WHERE candle_date >= '2019-02-01' AND candle_date < '2019-03-01'

        '''
        return q2

    def question_2(self, table_name='bars_1'):
        '''(dollar volume = AdjClose * Volume) in February 2019'''
        #February 2019
        #adj_close and volume
        #calculate average dollar volume
        command = self.query_2(table_name=table_name)
        ic(command)
        data = self.db_connector.execute(command=command, data=None, fetch=True, executemany=False)
        ic(len(data))
        ic(data[0:20])
        ic(data[-1])
        ic('average dollar volume in February 2019 is ', data[0][0])

    def query_3(self, table_name='bars_1'):
        #calc positive volume for each day
        #calc sum of positive volumes for
        #rank in ascending order
        q3 = '''
        WITH req_date AS (
        SELECT symbol, volume, adj_close, lag(adj_close) OVER (ORDER BY id) as adj_close_lag
        FROM bars_1
        WHERE candle_date >= '2015-01-01' AND candle_date <= '2015-12-31'
        )

        SELECT symbol, sum(volume) as volume_sum
        FROM req_date
        WHERE adj_close > adj_close_lag
        GROUP BY symbol
        ORDER BY volume_sum ASC

        '''
        return q3

    def question_3(self, table_name='bars_1'):
        '''Positive Volume(t) = Volume(t) if Adj.Close(t) >= Adj.Close(t-1) else 0. '''
        #2015
        #start from element 1 or 2
        #read adj_close
        #calc positive_volume
        #ascending order
        command = self.query_3(table_name=table_name)
        ic(command)
        data = self.db_connector.execute(command=command, data=None, fetch=True, executemany=False)
        ic(len(data))
        ic(data[0:20])
        ic(data[-1])
        ic('Rank of stocks in 2015 by Positive Volume in ascending order')
        for i in data:
            ic('Stock: ', i[0], 'Positive Volume: ', i[1])

    def query_4(self, table_name='bars_1'):
        #daily change = close(t) - close(t-1)
        #abs
        #mean
        #calculate for each stock
        q4 = '''
        WITH dc_abs AS (
        SELECT symbol, @(close - lag(close) OVER (ORDER BY id)) as daily_change
        FROM bars_1
        )

        SELECT symbol, AVG(daily_change) as avg_daily_change
        FROM dc_abs
        GROUP BY symbol
        ORDER BY avg_daily_change

        '''
        return q4

    def question_4(self, table_name='bars_1'):
        '''Average Absolute Daily Percent Change = Mean[Abs(Daily % changes for a stock)]'''
        #daily change = close(t) - close(t-1)
        #abs
        #mean
        #calculate for each stock
        command = self.query_4(table_name=table_name)
        ic(command)
        data = self.db_connector.execute(command=command, data=None, fetch=True, executemany=False)
        ic(len(data))
        ic(data[0:20])
        ic(data[-1])
        ic('Average absolute daily percent change for each stock.')
        for i in data:
            ic('Stock: ', i[0], 'AADPC: ', i[1])


class TestTask11:
    def __init__(self, config_filename, config_section):
        self.db_connector = PostgresConnector(config_filename, config_section)
        self.sql_queries = Task11(config_filename, config_section)

    def run(self):
        #data1 = self.db_connector.read_data(table_name='bars_1')
        #data2 = self.db_connector.read_data(table_name='bars_2')
        #ic('len of data1 tuple', len(data1))
        #ic('len of data2 tuple', len(data2))
        #ic('last element of data1:', data1[-1])
        #ic('last elementof data2:', data2[-1])

        self.sql_queries.question_1(table_name='bars_1')
        self.sql_queries.question_2(table_name='bars_1')
        self.sql_queries.question_3(table_name='bars_1')
        self.sql_queries.question_4(table_name='bars_1')


if __name__ == '__main__':
    zxc = TestTask11(config_filename='database.ini', config_section='postgresql')
    zxc.run()
