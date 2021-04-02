from enum import Enum, auto

from pymongo import MongoClient

from repository.repository import Repository


# _mongo_client = MongoClient('localhost', 27017)
_mongo_client = MongoClient('192.168.1.109', 27017)

class _DataType(Enum):
    CONTENT = 'content'
    DATA_FRAME = 'data_frame'


class MongoDBMeta(Enum):
    TWSE_PRICE_MEASUREMENT = ('TWSE', 'twse_price_measurement')
    TPEX_PRICE_MEASUREMENT = ('TWSE', 'tpex_price_measurement')
    DIVIDEND_POLICY = ('TWSE', 'dividend_policy')
    SIMPLE_BALANCE_SHEET = ('TWSE', 'balance_sheet')
    FULL_BALANCE_SHEET = ('TWSE', 'full_balance_sheet')
    STOCK_COUNT = ('TWSE', 'stock_count')
    CASH_FLOW = ('TWSE', 'cash_flow')
    SHARE_HOLDER = ('TWSE', 'shareholder_equity')
    DATAFRAME_PRICE_MEASUREMENT = ('TWSE', 'dataframe_price_measurement', _DataType.DATA_FRAME)

    def __init__(self, db, table, data_type=_DataType.CONTENT):
        self.db = db
        self.table = table
        self.data_type = data_type


class MongoDBRepository(Repository):
    def __init__(self, meta):
        self.meta = meta

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
            print("repository 3")
            record = collection.find_one({'$and': [{"stock_id": {"$eq": str(stock_id)}},
                                                   {"time_line": {"$eq": {'year': time_line['year'],
                                                                          'season': time_line['season']}}}]})
        if record is not None:
            print(record[self.meta.data_type.value])
            return record[self.meta.data_type.value]

    def put_data(self, stock_id, content, time_line=None):
        db = _mongo_client[self.meta.db]
        collection = db[self.meta.table]
        if content is None:
            return

        if time_line is None:
            collection.find_one_and_update({'stock_id': str(stock_id)}, {'$set': {self.meta.data_type.value: content}},
                                           upsert=True)
        else:
            collection = db[self.meta.table]
            collection.find_one_and_update({'$and': [{'stock_id': str(stock_id)}, {'time_line': time_line}]},
                                           {'$set': {"content": content}},
                                           upsert=True)


