import logging

from evaluation_utils import get_stock_codes
from evaluation_utils2 import sync_statements, sync_performance
from rdss.fetch_data_utils import get_raw_data, DB_TWSE, mongo_client, PATH_DIR_RAW_DATA_STOCK_COUNT, TABLE_STOCK_COUNT, \
    PATH_DIR_RAW_DATA_DIVIDEND_POLICY, TABLE_DIVIDEND_POLICY, fetch_tpex_price_measurement_raw_datas
from utils import get_time_lines, Offset
from value_measurement import TPEXPriceMeasurementTransformer, TWSEPriceMeasurementTransformer

logger = logging.getLogger('twse')
logger.setLevel(logging.DEBUG)
logger_console_handler = logging.StreamHandler()
logger_file_handler = logging.FileHandler('twse.log')
logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_console_handler.setFormatter(logger_formatter)
logger_file_handler.setFormatter(logger_formatter)
logger.addHandler(logger_console_handler)
logger.addHandler(logger_file_handler)


def move_quarterly_raw_datas_to_db(raw_data_path, table_name):
    code_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
    time_lines = get_time_lines(since={'year': 2013})
    db = mongo_client[DB_TWSE]
    for stock_id in code_list:
        for time_line in time_lines:
            print('put ', stock_id, ' time_line = ', time_line)
            year = time_line['year']
            season = time_line.get('season')
            raw_data = get_raw_data(raw_data_path + str(year) + "Q" + str(season), str(stock_id))
            if raw_data is not None:
                collection = db[table_name]
                collection.find_one_and_update({'$and': [{'stock_id': str(stock_id)}, {'time_line': time_line}]},
                                               {'$set': {"content": raw_data}},
                                               upsert=True)


def move_stock_count_raw_datas_to_db():
    code_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
    time_lines = get_time_lines(since={'year': 2013}, to={'year': 2020}, offset=Offset.YEAR)
    db = mongo_client[DB_TWSE]
    for stock_id in code_list:
        for time_line in time_lines:
            year = time_line['year']
            raw_data = get_raw_data(PATH_DIR_RAW_DATA_STOCK_COUNT + str(year), str(stock_id))
            if raw_data is not None:
                collection = db[TABLE_STOCK_COUNT]
                collection.find_one_and_update({'$and': [{'stock_id': str(stock_id)}, {'time_line': time_line}]},
                                               {'$set': {"content": raw_data}},
                                               upsert=True)


def move_dividend_policy_raw_datas_to_db():
    code_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
    db = mongo_client[DB_TWSE]
    for stock_id in code_list:
        raw_data = get_raw_data(PATH_DIR_RAW_DATA_DIVIDEND_POLICY, str(stock_id))
        if raw_data is not None:
            collection = db[TABLE_DIVIDEND_POLICY]
            collection.find_one_and_update({'$and': [{'stock_id': str(stock_id)}]},
                                           {'$set': {"content": raw_data}},
                                           upsert=True)


if __name__ == "__main__":
    logger.info('start')
    twse_code_list = get_stock_codes(stock_type='上市')
    tpex_code_list = get_stock_codes(stock_type='上櫃')
    sync_statements(twse_code_list)
    # sync_statements(tpex_code_list)
    # sync_performance(twse_code_list + tpex_code_list)