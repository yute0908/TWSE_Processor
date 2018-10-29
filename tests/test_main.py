import unittest

import requests
from bs4 import BeautifulSoup
from tabulate import tabulate

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementFetcher
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from fetcher import DataFetcher
from utils import get_time_lines
from value_measurement import ValueMeasurementProcessor


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
        income_statement_processor = SimpleIncomeStatementProcessor(2330)
        data_frame = income_statement_processor.get_data_frame(2018, 2)
        self.assertIsNotNone(data_frame)
        self.assertTrue(data_frame.loc['2018Q2', 'EPS'] is not None)
        self.assertTrue(data_frame.loc['2018Q2', '稅後淨利'] is not None)
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_request_balance_sheet(self):
        balance_sheet_processor = SimpleBalanceSheetProcessor(2330)
        data_frame = balance_sheet_processor.get_data_frame(2018, 2)
        self.assertIsNotNone(data_frame)
        self.assertTrue(data_frame.loc['2018Q2', '每股淨值'] is not None)

        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_dividend_policy(self):
        dividend_policy_processor = DividendPolicyProcessor(6294)
        data_frame = dividend_policy_processor.get_data_frame(2017, None)
        self.assertIsNotNone(data_frame)
        self.assertTrue(data_frame.loc[:, ['現金股利']] is not None)
        self.assertTrue(data_frame.loc[:, ['股息']] is not None)
        pass

    def test_value_measurement(self):
        value_measurement_processor = ValueMeasurementProcessor(4303)
        df = value_measurement_processor.get_data_frame()
        self.assertIsNotNone(df)
        print(df.loc[:, ['平均股價']])
        self.assertTrue(df.loc[:, ['平均股價']] is not None)

    def test_cash_flow_statemenst(self):
        start = {"year": 2016, "season": 2}
        self.get_data_frames(start)

    def get_data_frames(self, start):
        time_lines = get_time_lines(since=start)
        time_first = time_lines[0]
        if time_first.get('season') > 1:
            time_lines.insert(0, {'year': time_first.get('year'), 'season': (time_first.get('season') - 1)})
        print(len(time_lines))

        datafetcher = CashFlowStatementFetcher()
        result = datafetcher.fetch(None)
        if result.ok is False:
            return
        bs = BeautifulSoup(result.content, 'html.parser')
        table = bs.find_all('table', attrs={"class": "hasBorder", "align": "center"})

        print(table[0].prettify())
