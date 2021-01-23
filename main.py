import logging

from evaluation_utils import get_stock_codes
from rdss.fetch_data_utils import fetch_balance_sheet_raw_datas

logger = logging.getLogger('twse')
logger.setLevel(logging.DEBUG)
logger_console_handler = logging.StreamHandler()
logger_file_handler = logging.FileHandler('twse.log')
logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_console_handler.setFormatter(logger_formatter)
logger_file_handler.setFormatter(logger_formatter)
logger.addHandler(logger_console_handler)
logger.addHandler(logger_file_handler)

if __name__ == "__main__":
    logger.info('start')
    stock_code_list = get_stock_codes(stock_type='上市')
    stock_code_list.extend(get_stock_codes(stock_type='上櫃'))
    # fetch_simple_balance_sheet_raw_datas(stock_code_list[0: 4])
    # fetch_dividend_policy_raw_datas(stock_code_list, 2013)
    # fetch_stock_count_raw_datas(stock_code_list[0:5], since_year=2013)
    fetch_balance_sheet_raw_datas(stock_code_list)
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
