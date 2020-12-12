from os import path

from bs4 import BeautifulSoup

from rdss.fetcher import DataFetcher
from twse_crawler import gen_output_path
from utils import get_time_lines

PATH_DIR_RAW_DATA_BALANCE_SHEETS = "out/raw_datas/balance_sheets/"
PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS = "out/raw_datas/full_balance_sheets/"
PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY = "out/Vraw_datas/shareholder_equity/"

__balance_sheet_data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t164sb03')
__simple_balance_sheet_data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t163sb01')
__shareholder_equity_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t164sb06')

def get_shareholder_equity_raw_data(stock_id, year, season):
    get_shareholder_equity_raw_datas([stock_id], [{'year': year, 'season': season}])


def get_shareholder_equity_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __shareholder_equity_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id, 'year': year - 1911,
             'season': season})
        has_result = not (any(element.get_text() == "查無資料！" for element in
                              BeautifulSoup(result.content, 'html.parser').find_all('font')))

        return result.content if has_result else None

    __get_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY, fetcher)


def get_balance_sheet_raw_data(stock_id, year, season):
    get_balance_sheet_raw_datas([stock_id], [{'year': year, 'season': season}])


def get_balance_sheet_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __balance_sheet_data_fetcher.fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id",
             "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": stock_id, "year": year - 1911, "season": season})
        content = BeautifulSoup(result.content, 'html.parser').find_all('input')
        need_to_get_next = any(field['type'] == 'button' for field in content)
        if need_to_get_next:
            result = __balance_sheet_data_fetcher.fetch(
                {"encodeURIComponent": 1, "step": 2, "firstin": 1, "TYPEK": "sii", "co_id": stock_id,
                 "year": year - 1911, "season": season})
        has_result = not (any(element.get_text() == "查無所需資料！" for element in
                              BeautifulSoup(result.content, 'html.parser').find_all('font')))
        return result.content if has_result else None

    __get_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS, fetcher)


def get_simple_balance_sheet_raw_data(stock_id, year, season):
    get_simple_balance_sheet_raw_datas([stock_id], [{'year': year, 'season': season}])


def get_simple_balance_sheet_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __simple_balance_sheet_data_fetcher.fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id",
             "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": stock_id, "year": year - 1911, "season": season})
        has_result = not (any(element.get_text() == "查詢無資料" for element in
                              BeautifulSoup(result.content, 'html.parser').find_all('font')))
        return result.content if has_result else None
    __get_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_BALANCE_SHEETS, fetcher)


def __get_datas_and_store(stock_ids, time_lines, root_dir_path, fetcher):
    def action(stock_id, year, season):
        dir_path = root_dir_path + str(year) + "Q" + str(season)
        file_path = gen_output_path(dir_path, str(stock_id))
        if path.exists(file_path) is False:
            result = fetcher(stock_id, year, season)
            if result is not None:
                store_raw_data(result, dir_path, str(stock_id))

    __iterate_all(stock_ids, time_lines, action)



def __iterate_all(stock_ids, time_lines, action):
    for stock_id in stock_ids:
        for time_line in time_lines:
            action(stock_id, time_line['year'], time_line['season'])


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
