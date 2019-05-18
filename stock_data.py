import traceback

import pandas as pd

from twse_crawler import gen_output_path


class StockData:
    def __init__(self, stock_id, df_performance, df_statement, df_profit_matrix):
        super().__init__()
        self.stock_id = stock_id
        self.df_performance = df_performance
        self.df_statement = df_statement
        self.df_profit_matrix = df_profit_matrix


def read(stock_id, path=None):
    dfs_stock_data = read_dfs(stock_id, path)
    if dfs_stock_data is None:
        return None
    else:
        df_performance = dfs_stock_data.get('performance')
        df_statement = dfs_stock_data.get('statement')
        df_profit_matrix = dfs_stock_data.get('profit_matrix')

        if df_performance is None and df_statement is None and df_profit_matrix is None:
            return None
        else:
            return StockData(stock_id, df_performance, df_statement, df_profit_matrix)


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
    store_df(str(stockData.stock_id), {'statement': stockData.df_statement, 'performance': stockData.df_performance,
                                       'profit_matrix': stockData.df_profit_matrix}, directory, filename)


def store_df(stock_id, dict_sheet_dfs, directory=None, filename=None):
    if dict_sheet_dfs is None or len(dict_sheet_dfs.items()) == 0 or stock_id is None:
        print('dict_sheet_dfs and stock_id should not be None')
        return

    output_path = gen_output_path(directory if directory is not None else 'data',
                                  filename=filename if filename is not None else 'performance_{0}.xlsx'.format(
                                      stock_id))
    dict_dfs = read_dfs(stock_id, output_path)
    if dict_dfs is None:
        dict_dfs = {}

    for sheet_name, df in dict_sheet_dfs.items():
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
