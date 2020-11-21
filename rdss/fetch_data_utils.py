from os import path

from bs4 import BeautifulSoup

from rdss.fetcher import DataFetcher
from twse_crawler import gen_output_path
from utils import get_time_lines

PATH_DIR_RAW_DATA_BALANCE_SHEETS = "raw_datas/balance_sheets/"


def get_simple_balance_sheet_raw_data(stock_id, year, season):
    get_simple_balance_sheet_raw_datas([stock_id], [{'year': year, 'season': season}])


def get_simple_balance_sheet_raw_datas(stock_ids, time_lines=get_time_lines()):
    simple_data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t163sb01')
    for stock_id in stock_ids:
        for time_line in time_lines:
            year = time_line['year']
            season = time_line['season']
            dir_path = PATH_DIR_RAW_DATA_BALANCE_SHEETS + str(year) + "Q" + str(season)
            file_path = gen_output_path(dir_path, str(stock_id))
            path_exists = path.exists(file_path)
            if path_exists is False:
                result = simple_data_fetcher.fetch(
                    {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id", "inpuType": "co_id",
                     "TYPEK": "all", "isnew": "false", "co_id": stock_id, "year": year - 1911, "season": season})
                has_result = not (any(element.get_text() == "查詢無資料" for element in BeautifulSoup(result.content, 'html.parser').find_all('font')))
                if has_result:
                    store_raw_data(result.content, dir_path, str(stock_id))


def store_raw_data(data, output_dir, file_name):
    if data is not None:
        output_path = gen_output_path(output_dir, file_name)
        with open(output_path, 'wb') as output:
            output.write(data)
            output.close()


def get_raw_data(input_dir, file_name):
    input_path = gen_output_path(input_dir, file_name)
    with open(input_path, 'rb') as in_put:
        raw_input = in_put.read()
        in_put.close()
        return raw_input
