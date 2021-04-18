from enum import Enum, auto

import pandas as pd
from pymongo import MongoClient

from repository.repository import Repository

# _mongo_client = MongoClient('localhost', 27017)
_mongo_client = MongoClient('192.168.1.109', 27017)


class _DataType(Enum):
    CONTENT = 'content'
    DATA_FRAME = 'data_frame'


default_transform = lambda x: x


class Transformer:
    def __init__(self, in_transform=default_transform, out_transform=default_transform):
        self.in_transform = in_transform
        self.out_transform = out_transform


default_transformer = Transformer()


def default_data_frame_out_transform(data_frame):
    return data_frame.to_json(orient='split')


def quarterly_period_data_frame_in_transform(content):
    data_frame = pd.read_json(content, orient='split', typ='frame')
    period_strings = list(
        map(lambda _dict: '{}-{}-{}'.format(_dict['qyear'], _dict['month'], _dict['day']), data_frame.index))
    data_frame.set_index(pd.DatetimeIndex(period_strings).to_period('Q'), inplace=True)
    return data_frame

def yearly_period_data_frame_in_transform(content):
    data_frame = pd.read_json(content, orient='split', typ='frame')
    # print(data_frame)
    period_strings = list(map(lambda _dict: '{}'.format(_dict['qyear']), data_frame.index))
    data_frame.set_index(pd.DatetimeIndex(period_strings).to_period('Y'), inplace=True)
    # print(data_frame)
    # print(data_frame.index)
    return data_frame


def quarterly_period_balance_sheet_data_frame_in_transform(content):
    data_frame = pd.read_json(content, orient='split', typ='frame')
    period_strings = list(
        map(lambda _dict: '{}-{}-{}'.format(_dict['qyear'], _dict['month'], _dict['day']), data_frame.index))
    data_frame.set_index(pd.DatetimeIndex(period_strings).to_period('Q'), inplace=True)
    # print(data_frame)
    data_frame.columns = [tuple(column.split(',')) if ',' in column else column for column in data_frame.columns]
    # print(data_frame)
    return data_frame


def quarterly_period_balance_sheet_data_frame_out_transform(data_frame):
    columns = data_frame.columns
    # print(columns)
    new_columns = [','.join(t) if (isinstance(t, tuple)) else t for t in columns]
    # print(new_columns)
    data_frame.columns = new_columns
    data_frame_json = data_frame.to_json(orient='split')
    # print(data_frame_json)
    return data_frame_json


class MongoDBMeta(Enum):
    TWSE_PRICE_MEASUREMENT = ('TWSE', 'twse_price_measurement')
    TPEX_PRICE_MEASUREMENT = ('TWSE', 'tpex_price_measurement')
    DIVIDEND_POLICY = ('TWSE', 'dividend_policy')
    SIMPLE_BALANCE_SHEET = ('TWSE', 'balance_sheet')
    FULL_BALANCE_SHEET = ('TWSE', 'full_balance_sheet')
    STOCK_COUNT = ('TWSE', 'stock_count')
    CASH_FLOW = ('TWSE', 'cash_flow')
    SHARE_HOLDER = ('TWSE', 'shareholder_equity')
    DATAFRAME_PRICE_MEASUREMENT = ('TWSE', 'dataframe_price_measurement', _DataType.DATA_FRAME,
                                   Transformer(in_transform=default_data_frame_out_transform))
    DATAFRAME_CASH_FLOW = ('TWSE', 'dataframe_cash_flow', _DataType.DATA_FRAME,
                           Transformer(in_transform=quarterly_period_data_frame_in_transform,
                                       out_transform=default_data_frame_out_transform)
                           )
    DATAFRAME_PROFIT_STATEMENT = ('TWSE', 'dataframe_profit_statement', _DataType.DATA_FRAME,
                                  Transformer(in_transform=quarterly_period_data_frame_in_transform,
                                              out_transform=default_data_frame_out_transform)
                                  )
    DATAFRAME_BALANCE_SHEET = ('TWSE', 'dataframe_balance_sheet', _DataType.DATA_FRAME,
                               Transformer(in_transform=quarterly_period_balance_sheet_data_frame_in_transform,
                                           out_transform=quarterly_period_balance_sheet_data_frame_out_transform)
                               )
    DATAFRAME_DIVIDEND_POLICY = ('TWSE', 'dataframe_dividend_policy', _DataType.DATA_FRAME,
                                 Transformer(in_transform=yearly_period_data_frame_in_transform,
                                             out_transform=default_data_frame_out_transform)
                                 )

    def __init__(self, db, table, data_type=_DataType.CONTENT, transformer=default_transformer):
        self.db = db
        self.table = table
        self.data_type = data_type
        self.transformer = transformer


class MongoDBRepository(Repository):
    def __init__(self, meta, transformer=None):
        self.meta = meta
        self.__transformer = transformer if transformer is not None else meta.transformer

    def get_data(self, stock_id, time_line=None):
        db = _mongo_client[self.meta.db]
        collection = db[self.meta.table]
        if time_line is None:
            print("repository 1")
            record = collection.find_one({"stock_id": {"$eq": str(stock_id)}})
        elif time_line.get('season') is None:
            print("repository 2 time_line = ", time_line)
            record = collection.find_one({'$and': [{"stock_id": {"$eq": str(stock_id)}},
                                                   {"time_line": {"$eq": {'year': time_line['year']}}}]})
        else:
            print("repository 3 stock_id = ", stock_id, ' year = ', time_line['year'], ' season = ', time_line['season'])
            record = collection.find_one({'$and': [{"stock_id": {"$eq": str(stock_id)}},
                                                   {"time_line": {"$eq": {'year': time_line['year'],
                                                                          'season': time_line['season']}}}]})
        if record is not None:
            print(record[self.meta.data_type.value])
            return self.__transformer.in_transform(record[self.meta.data_type.value])

    def put_data(self, stock_id, content, time_line=None):
        db = _mongo_client[self.meta.db]
        collection = db[self.meta.table]
        if content is None:
            return

        content = self.__transformer.out_transform(content)
        if time_line is None:
            collection.find_one_and_update({'stock_id': str(stock_id)}, {'$set': {self.meta.data_type.value: content}},
                                           upsert=True)
        else:
            collection = db[self.meta.table]
            collection.find_one_and_update({'$and': [{'stock_id': str(stock_id)}, {'time_line': time_line}]},
                                           {'$set': {"content": content}},
                                           upsert=True)
