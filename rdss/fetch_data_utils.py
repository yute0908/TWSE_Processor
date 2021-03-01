import logging
import os
import time
import traceback
from datetime import datetime
from os import path
from urllib.parse import urlencode
from urllib.request import urlopen, ProxyHandler, build_opener, install_opener, Request

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver import FirefoxProfile
from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config

from rdss.fetcher import DataFetcher
from twse_crawler import gen_output_path
from utils import get_time_lines, Offset

PATH_DIR_RAW_DATA_SIMPLE_BALANCE_SHEETS = "out/raw_datas/balance_sheets/"
PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS = "out/raw_datas/full_balance_sheets/"
PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY = "out/raw_datas/shareholder_equity/"
PATH_DIR_RAW_DATA_DIVIDEND_POLICY = "out/raw_datas/dividend_policy"
PATH_DIR_RAW_DATA_STOCK_COUNT = "out/raw_datas/stock_count/"
PATH_DIR_RAW_DATA_CASH_FLOW = "out/raw_datas/cash_flow/"
PATH_DIR_RAW_DATA_PRICE_MEASUREMENT = "out/raw_datas/price_measurement/"

__balance_sheet_data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t164sb03')
__simple_balance_sheet_data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t163sb01')
__shareholder_equity_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t164sb06')
__dividend_policy_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t05st09_2')
__stock_count_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t16sn02')
__cash_flow_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t164sb05')

__logger = logging.getLogger("twse.DataFetcher")

mongo_client = MongoClient('localhost', 27017)
# mongo_client = MongoClient('192.168.1.109', 27017)
DB_TWSE = "TWSE"
TABLE_PRICE_MEASUREMENT = "price_measurement"

proxy_port = 9050
ctrl_port = 9051


def _tor_process_exists():
    try:
        ctrl = Controller.from_port(port=ctrl_port)
        ctrl.close()
        return True
    except:
        return False


def _launch_tor():
    return launch_tor_with_config(
        config={
            'SOCKSPort': str(proxy_port),
            'ControlPort': str(ctrl_port)
        },
        take_ownership=True)


def fetch_stock_count_raw_data(stock_id, since_year, to_year):
    fetch_balance_sheet_raw_datas([stock_id], since_year, to_year)


def fetch_stock_count_raw_datas(stock_ids, since_year=datetime.now().year, to_year=datetime.now().year):
    time_lines = get_time_lines(since={'year': since_year}, to={'year': to_year}, offset=Offset.YEAR)

    def fetcher(stock_id, year):
        result = __stock_count_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id',
             't05st29_c_ifrs': 'N',
             't05st30_c_ifrs': 'N', 'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id,
             'year': (year - 1911)}
        )
        return result.content

    __fetch_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_STOCK_COUNT, fetcher)


class TorHandler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'}

    def set_url_proxy(self):
        proxy_support = ProxyHandler({'http': '127.0.0.1:8118'})
        opener = build_opener(proxy_support)
        install_opener(opener)

    def open_url(self, url):
        # communicate with TOR via a local proxy (privoxy)

        self.set_url_proxy()
        request = Request(url, None, self.headers)
        return urlopen(request).read().decode('utf-8')

    def renew_connection(self):
        wait_time = 2
        number_of_ip_rotations = 3
        session = requests.session()
        session.proxies = {'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'}
        ip = session.get("http://icanhazip.com").text
        print('My first IP: {}'.format(ip))
        # for i in range(0, number_of_ip_rotations):
        old_ip = ip
        seconds = 0

        # Loop until the 'new' IP address is different than the 'old' IP address,
        # It may take the TOR network some time to effect a different IP address
        while ip == old_ip:
            time.sleep(wait_time)
            seconds += wait_time
            print('{} seconds elapsed awaiting a different IP address.'.format(seconds))

            # http://icanhazip.com/ is a site that returns your IP address
            with Controller.from_port(port=9051) as controller:
                controller.authenticate("my-tor-password")
                controller.signal(Signal.NEWNYM)
                controller.close()
            ip = session.get("http://icanhazip.com").text
            print('My new IP: {}'.format(ip))


def fetch_twse_price_measurement_raw_datas(stock_ids):
    tor_handler = TorHandler()
    session = requests.session()
    session.proxies = {'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'}

    # Cycle through the specified number of IP addresses via TOR
    index = 0

    # for stock_id in stock_ids:
    tor_handler.renew_connection()

    while index < len(stock_ids):
        stock_id = stock_ids[index]
        try:
            # ip = session.get("http://icanhazip.com").text
            # print('My IP: {}'.format(ip))
            result = session.request(method='GET', url="https://www.twse.com.tw/exchangeReport/FMNPTK",
                                     params={"response": "json", "stockNo": str(stock_id)},
                                     headers={'Connection': 'close'}, timeout=10)
            print('stock_id = ', stock_id, ' success = ', result.ok)
            print('content = ', result.content)
            print('json = ', result.json())
        except Exception as inst:
            __logger.error("get exception in " + str(stock_id) + ":" + str(inst))
            traceback.print_tb(inst.__traceback__)
            result = None

        if result is None or not result.ok:
            tor_handler.renew_connection()
        else:
            db = mongo_client[DB_TWSE]
            collection = db[TABLE_PRICE_MEASUREMENT]
            collection.find_one_and_update({'stock_id': str(stock_id)}, {'$set': {"content": result.json()}},
                                           upsert=True)
            index += 1

def fetch_tpex_price_measurement_raw_datas(stock_ids):
    index = 0
    fetcher = DataFetcher("https://www.tpex.org.tw/web/stock/statistics/monthly/result_st42.php?l=zh-tw")
    while index < len(stock_ids):
        stock_id = stock_ids[index]
        result = fetcher.fetch({'ajax': 'true', 'input_stock_code': str(stock_id)})
        index += 1
        print('stock id ', stock_id, ' result = ', result.content)


def fetch_price_measurement_raw_datas(stock_ids):
    db = mongo_client[DB_TWSE]
    collection = db[TABLE_PRICE_MEASUREMENT]
    for stock_id in stock_ids:
        params = {'STOCK_ID': stock_id}
        path_dir = os.path.abspath(gen_output_path(PATH_DIR_RAW_DATA_PRICE_MEASUREMENT))
        url = "https://goodinfo.tw/StockInfo/StockBzPerformance.asp?" + urlencode(params)
        print('url = ', url)
        profile = FirefoxProfile()
        profile.set_preference("browser.download.panel.shown", False)
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.dir", path_dir)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/html")
        driver = webdriver.Firefox(firefox_profile=profile)
        driver.get(url)
        inputs = driver.find_elements_by_tag_name("input")
        download_button = None
        for input_ in inputs:
            if input_.get_attribute("value") == '匯出HTML':
                download_button = input_
        download_button.click()
        driver.quit()
        file_path = os.path.join(path_dir, "BzPerformance.html")
        try:
            output_file_path = gen_output_path(PATH_DIR_RAW_DATA_PRICE_MEASUREMENT, str(stock_id))
            # copyfile(file_path, output_file_path)
            with open(file_path, 'rb') as in_put:
                raw_input = in_put.read()
                in_put.close()
                print(raw_input)
                record = {
                    "stock_id": str(stock_id),
                    "content": raw_input
                }
                post_id = collection.insert_one(record)
                print("post_id = ", post_id)

        except Exception as inst:
            __logger.error("get exception in " + str(stock_id) + ":" + str(inst))
            traceback.print_tb(inst.__traceback__)
        finally:
            os.remove(file_path)


def fetch_dividend_policy_raw_data(stock_id, since_year, to_year):
    fetch_dividend_policy_raw_datas([stock_id], since_year, to_year)


def fetch_dividend_policy_raw_datas(stock_ids, since_year=2013, to_year=datetime.now().year):
    for stock_id in stock_ids:
        result = __dividend_policy_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id, 'date1': (since_year - 1911),
             'date2': (to_year - 1911), 'qryType': 2, 'firstin': 1})
        store_raw_data(result.content, PATH_DIR_RAW_DATA_DIVIDEND_POLICY, str(stock_id))


def fetch_shareholder_equity_raw_data(stock_id, year, season):
    fetch_shareholder_equity_raw_datas([stock_id], [{'year': year, 'season': season}])


def fetch_shareholder_equity_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __shareholder_equity_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id, 'year': year - 1911,
             'season': season})
        parser = BeautifulSoup(result.content, 'html.parser')
        has_result = not (any(element.get_text() == "查無資料！" for element in parser.find_all('font')))
        has_button = len(parser.find_all('input')) > 0
        if has_result and has_button:
            result = __shareholder_equity_fetcher.fetch(
                {'encodeURIComponent': 1, 'TYPEK': 'sii', 'step': 2, 'year': year - 1911, 'season': season,
                 'co_id': stock_id, 'firstin': 1})
            parser = BeautifulSoup(result.content, 'html.parser')
            has_result = not (any(element.get_text() == "查無資料！" for element in parser.find_all('font')))
        return result.content if has_result else None

    __fetch_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_SHAREHOLDER_EQUITY, fetcher)


def fetch_balance_sheet_raw_data(stock_id, year, season):
    fetch_balance_sheet_raw_datas([stock_id], [{'year': year, 'season': season}])


def fetch_balance_sheet_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
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

    __fetch_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_FULL_BALANCE_SHEETS, fetcher)


def fetch_simple_balance_sheet_raw_data(stock_id, year, season):
    fetch_simple_balance_sheet_raw_datas([stock_id], [{'year': year, 'season': season}])


def fetch_simple_balance_sheet_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __simple_balance_sheet_data_fetcher.fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id",
             "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": stock_id, "year": year - 1911, "season": season})
        has_result = not (any(element.get_text() == "查詢無資料" for element in
                              BeautifulSoup(result.content, 'html.parser').find_all('font')))
        return result.content if has_result else None

    __fetch_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_SIMPLE_BALANCE_SHEETS, fetcher)


def fetch_cash_flow_raw_data(stock_id, year, season):
    fetch_cash_flow_raw_datas([stock_id], [{'year': year, 'season': season}])


def fetch_cash_flow_raw_datas(stock_ids, time_lines=get_time_lines(since={'year': 2013})):
    def fetcher(stock_id, year, season):
        result = __cash_flow_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id, 'year': year - 1911,
             'season': season}
        )
        inputs_tag = BeautifulSoup(result.content, 'html.parser').find_all('input')
        need_to_get_next = any(field['type'] == 'button' for field in inputs_tag)
        if need_to_get_next:
            result = __cash_flow_fetcher.fetch(
                {"encodeURIComponent": 1, "step": 2, "firstin": 1, "TYPEK": "sii", "co_id": stock_id,
                 "year": year - 1911, "season": season}
            )
        has_result = not (any(element.get_text() == "查詢無資料" or element.get_text() == '查無所需資料！' for element in
                              BeautifulSoup(result.content, 'html.parser').find_all('font')))
        return result.content if has_result else None

    __fetch_datas_and_store(stock_ids, time_lines, PATH_DIR_RAW_DATA_CASH_FLOW, fetcher)


def __fetch_datas_and_store(stock_ids, time_lines, root_dir_path, fetcher):
    def action(stock_id, time_line):
        year = time_line['year']
        season = time_line.get('season')
        dir_path = (root_dir_path + str(year)) if season is None else (root_dir_path + str(year) + "Q" + str(season))
        file_path = gen_output_path(dir_path, str(stock_id))
        if path.exists(file_path) is False:
            result = fetcher(stock_id, year) if season is None else fetcher(stock_id, year, season)
            if result is not None:
                store_raw_data(result, dir_path, str(stock_id))

    __iterate_all(stock_ids, time_lines, action)


def __iterate_all(stock_ids, time_lines, action):
    for stock_id in stock_ids:
        for time_line_item in time_lines:
            action(stock_id, time_line_item)


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
