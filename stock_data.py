import traceback

import pandas as pd

from twse_crawler import gen_output_path


class StockData:
    def __init__(self, stock_id, df_performance, df_statement):
        super().__init__()
        self.stock_id = stock_id
        self.df_performance = df_performance
        self.df_statement = df_statement


def read(stock_id, path=None):
    try:
        file = open(path if path is not None else gen_output_path('data', 'performance_{0}.xlsx'.format(str(stock_id))),
                    'rb')
        return StockData(stock_id, pd.read_excel(file, sheet_name='performance'),
                         pd.read_excel(file, sheet_name='statement'))
    except FileNotFoundError as err:
        return None


def read_profit_matrix(stock_id, path=None):
    try:
        with open(path if path is not None else gen_output_path('data', 'performance_{0}.xlsx'.format(str(stock_id))),
                    'rb') as file:
            df_profit_matrix = pd.read_excel(file, sheet_name='profit_matrix')
            file.close()
            return df_profit_matrix
    except FileNotFoundError as err:
        traceback.print_tb(err.__traceback__)
        return None


def store(stockData, directory=None, filename=None):
    if stockData is None:
        print('stockData and directory should not be None')
        return
    output_path = gen_output_path(directory if directory is not None else 'data',
                                  filename=filename if filename is not None else 'performance_{0}.xlsx'.format(
                                      str(stockData.stock_id)))
    with pd.ExcelWriter(output_path) as writer:
        stockData.df_statement.to_excel(writer, sheet_name='statement')
        stockData.df_performance.to_excel(writer, sheet_name='performance')


def store_df(stock_id, df, sheet_name, directory=None, filename=None):

    if sheet_name is None or df is None or stock_id is None:
        print('sheet_name and df and stock_id should not be None')
        return

    output_path = gen_output_path(directory if directory is not None else 'data',
                                  filename=filename if filename is not None else 'performance_{0}.xlsx'.format(
                                      stock_id))
    dict_dfs = read_dfs(stock_id, output_path)
    if dict_dfs is None:
        dict_dfs = {}
    dict_dfs[sheet_name] = df

    with pd.ExcelWriter(output_path) as writer:
        for sheet_name, df in dict_dfs.items():
            df.to_excel(writer, sheet_name=sheet_name)
        writer.close()


def read_dfs(stock_id, path=None):
    try:
        with open(path if path is not None else gen_output_path('data', 'performance_{0}.xlsx'.format(str(stock_id))),
                    'rb') as file:
            df_profit_matrix = pd.read_excel(file, sheet_name=None)
            file.close()
            return df_profit_matrix
    except FileNotFoundError as err:
        traceback.print_tb(err.__traceback__)
        return None