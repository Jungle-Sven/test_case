import pandas as pd

class ShuffleData:
    '''There was no common symbols in bars_1 and bars_2,
    so Test Case 1.2 could not be finished.

    ShuffleData is a class to mix some data in bars_1 and bars_2.

    ShuffleData takes 1000 random trades from both csv files
    and adds a half(500) to each dataset

    output files: bars_1_shuffled, bars_2_shuffled.
    '''
    def __init__(self):
        pass

    def read_data(self, filename):
        data = pd.read_csv(filename)
        return data

    def create_df(self, data):
        df = pd.DataFrame(data)
        return df

    def add_dfs(self, df1, df2):
        return pd.concat([df1, df2], ignore_index=True, sort=False)

    def shuffle(self, df):
        df_shuffled = df.sample(n=1000)
        '''add 500 rows of mixed data to each csv '''
        len_df = int(len(df)/2)
        len_df_shuffled = int(len(df_shuffled)/2)
        df1 = pd.concat([df.head(len_df), df_shuffled.head(len_df_shuffled)], ignore_index=True, sort=False)
        df2 = pd.concat([df.tail(len_df), df_shuffled.tail(len_df_shuffled)], ignore_index=True, sort=False)
        return df1, df2

    def save_df(self, df, filename):
        df.to_csv(filename, index=False)

    def run(self):
        data1 = self.read_data('bars_1.csv')
        df1 = self.create_df(data1)
        data2 = self.read_data('bars_2.csv')
        df2 = self.create_df(data2)
        df_merged = self.add_dfs(df1, df2)
        df1, df2 = self.shuffle(df_merged)
        self.save_df(df1, 'bars_1_shuffled.csv')
        self.save_df(df2, 'bars_2_shuffled.csv')

if __name__ == '__main__':
    pass
