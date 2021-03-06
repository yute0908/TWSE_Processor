import logging

from evaluation_utils import get_stock_codes
from rdss.fetch_data_utils import fetch_twse_price_measurement_raw_datas, fetch_tpex_price_measurement_raw_datas, \
    PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS, get_raw_data, DB_TWSE, mongo_client, TABLE_SIMPLE_BALANCE_SHEET, \
    TABLE_FULL_BALANCE_SHEET, PATH_DIR_RAW_DATA_SIMPLE_BALANCE_SHEETS, PATH_DIR_RAW_DATA_CASH_FLOW, TABLE_CASH_FLOW, \
    PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY, TABLE_SHAREHOLDER_EQUITY, PATH_DIR_RAW_DATA_STOCK_COUNT, TABLE_STOCK_COUNT, \
    PATH_DIR_RAW_DATA_DIVIDEND_POLICY, TABLE_DIVIDEND_POLICY
from utils import get_time_lines, Offset
from value_measurement import TPEXPriceMeasurementProcessor

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
    for stock_id in code_list[0: 10]:
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
    # fetch_twse_price_measurement_raw_datas(stock_code_list)
    # fetch_tpex_price_measurement_raw_datas(tpex_code_list)
    # move_quarterly_raw_datas_to_db(PATH_DIR_RAW_DATA_SIMPLE_BALANCE_SHEETS, TABLE_SIMPLE_BALANCE_SHEET)
    # move_quarterly_raw_datas_to_db(PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS, TABLE_FULL_BALANCE_SHEET)
    # move_quarterly_raw_datas_to_db(PATH_DIR_RAW_DATA_CASH_FLOW, TABLE_CASH_FLOW)
    # move_quarterly_raw_datas_to_db(PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY, TABLE_SHAREHOLDER_EQUITY)
    # move_stock_count_raw_datas_to_db()
    # move_dividend_policy_raw_datas_to_db()
    # tpex_price_measurement_processor = TPEXPriceMeasurementProcessor()
    # tpex_price_measurement_processor.get_data_frame(tpex_code_list[0])

    # db = mongo_client[DB_TWSE]
    # collection = db[TABLE_PRICE_MEASUREMENT]
    # record = collection.find_one({"stock_id": str(1101)})
    # print(type(record['content']))
    # print(record['content'])
    # fetch_price_measurement_raw_datas([2809])
    # fetch_simple_balance_sheet_raw_datas(stock_code_list[0: 4])
    # fetch_dividend_policy_raw_datas(stock_code_list, 2013)
    # fetch_stock_count_raw_datas(stock_code_list[0:5], since_year=2013)
    # fetch_balance_sheet_raw_datas(stock_code_list)
    # client = MongoClient('localhost', 27017)
    # db = client.test_database
    # post = {"author": "Mike",
    #         "text": "My first blog post!",
    #         "tags": ["mongodb", "python", "pymongo"],
    #         "date": datetime.datetime.utcnow()}
    # posts = db.posts
    # post_id = posts.insert_one(post).inserted_id
    # print('post_id = ', post_id)
    # print(db.list_collection_names())
    # time_lines = get_time_lines(since={'year': 2013})
    # input_path = gen_output_path(PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY + '2020Q3', '2809')
    # for time_line in time_lines:
    #     for stock_id in stock_code_list:
    #         year = time_line['year']
    #         season = time_line.get('season')
    #         input_path = gen_output_path(PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY + str(year) + 'Q' + str(season), str(stock_id))
    #         if os.path.exists(input_path):
    #             with open(input_path, 'rb') as in_put:
    #                 raw_input = in_put.read()
    #                 in_put.close()
    #                 content = BeautifulSoup(raw_input, 'html.parser')
    #                 if content.contains_replacement_characters:
    #                     logger.info('wrong format ' + input_path)
    #                     os.remove(input_path)
