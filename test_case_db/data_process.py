import time

from data_shuffle import ShuffleData
from db_connector import PostgresConnector

class DataProcessing:
    '''reads csv and processes data to database '''
    def __init__(self, config_filename, config_section):
        self.db_connector = PostgresConnector(config_filename, config_section)
        self.shuffle = ShuffleData()

    def test_insert_data_executemany(self, source_filename='bars_1.csv', table_name='bars_1'):
        data = self.db_connector.preprocess_data(source_filename)
        self.db_connector.insert_data_executemany(data, table_name)

    def run(self):
        print('hello data reader')
        time.sleep(15)
        '''actions:
        delete tables if they exist
        take data from csv and mix it with ShuffleData
        insert data to postgress database '''
        self.db_connector.delete_tables()

        self.db_connector.create_tables()

        self.shuffle.run()

        self.test_insert_data_executemany(source_filename='bars_1_shuffled.csv', table_name='bars_1')
        self.test_insert_data_executemany(source_filename='bars_2_shuffled.csv', table_name='bars_2')

        print('Done. All data processed to database.')


if __name__ == '__main__':
    process_data = DataProcessing(config_filename='database.ini', config_section='postgresql')
    process_data.run()
