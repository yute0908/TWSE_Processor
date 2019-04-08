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
