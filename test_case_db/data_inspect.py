import pandas as pd

from icecream import ic

'''a simple class to have a look at our dataset '''
class DataInspector:
    def __init__(self):
        pass

    def read_data(self, filename):
        data = pd.read_csv(filename)
        return data

    def create_df(self, data):
        df = pd.DataFrame(data)
        return df

    def find_list_of_symbols(self, df):
        ic(df)
        return df['Symbol'].unique()

    def check_common(self, list1, list2):
        common_elements = [i for i in list1 if i in list2]
        return common_elements

    def run(self):
        data = self.read_data(filename='bars_1.csv')
        df = self.create_df(data=data)
        list_of_symbols = self.find_list_of_symbols(df=df)
        #ic(list_of_symbols)
        data2 = self.read_data(filename='bars_2.csv')
        df2 = self.create_df(data=data2)
        list_of_symbols2 = self.find_list_of_symbols(df=df2)

        common_elements = self.check_common(list_of_symbols, list_of_symbols2)
        ic(common_elements)

if __name__ == '__main__':
    di = DataInspector()
    di.run()
