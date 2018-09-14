import unittest

import requests
from tabulate import tabulate

from rdss.processors import BalanceSheetProcessor, IncomeStatementProcessor
from fetcher import DataFetcher


class MainTest(unittest.TestCase, DataFetcher):

    def test_requestUrl(self):
        url = 'http://mops.twse.com.tw/mops/web/t51sb01'
        s = requests.Session()
        r = s.get(url)
        print(r)

    def test_request_post_url(self):
        data_fetcher = DataFetcher('http://mops.twse.com.tw/mops/web/ajax_t164sb03')
        params = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'queryName': 'co_id',
                  'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false', 'co_id': '2330', 'year': 107, 'season': 2}

        result = data_fetcher.fetch(params)
        self.assertTrue(result.ok)
        print(result.content)

    def test_request_income_statement(self):
        income_statement_processor = IncomeStatementProcessor(2330)
        data_frame = income_statement_processor.get_data_frame(107, 2)
        self.assertIsNotNone(data_frame)
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))


    def test_request_balance_sheet(self):
        balance_sheet_processor = BalanceSheetProcessor(2330)
        data_frame = balance_sheet_processor.get_data_frame(107, 2)
        self.assertIsNotNone(data_frame)
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
