import pandas as pd
import unittest

import requests
from bs4 import BeautifulSoup
from tabulate import tabulate

import roe_utils
from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import _CashFlowStatementFetcher, CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from fetcher import DataFetcher
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from utils import get_time_lines
from value_measurement import PriceMeasurementProcessor


class MainTest(unittest.TestCase):

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
        # self.assertIsNotNone(data_frame)
        # self.assertTrue(data_frame.loc['2018Q2', 'EPS'] is not None)
        # self.assertTrue(data_frame.loc['2018Q2', '稅後淨利'] is not None)
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        data_frame = income_statement_processor.get_data_frames(since={'year': 2017, 'season': 1},
                                                                to={'year': 2018, 'season': 2})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_shareholder_equity(self):
        shareholder_euity_processor = ShareholderEquityProcessor(2330)
        # data_frame = shareholder_euity_processor.get_data_frame(2018, 1)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        #
        # data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2018})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        #
        # data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2018, 'season': 2})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2017})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2017, 'season': 2})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2018, 'season': 3})
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

    def test_price_measurement(self):
        price_measurement_processor = PriceMeasurementProcessor(4303)
        df = price_measurement_processor.get_data_frame()
        self.assertIsNotNone(df)
        print(df.loc[:, ['平均股價']])
        self.assertTrue(df.loc[:, ['平均股價']] is not None)

    def test_cash_flow_statement(self):
        cash_flow_processor = CashFlowStatementProcessor(2330)
        # data_frame = cash_flow_processor.get_data_frame(2017, 2)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        # cash_flow_processor.get_data_frames(since={"year": 2016, "season": 2})

        data_frame = cash_flow_processor.get_data_frames({'year': 2018})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        stock_count_processor = StockCountProcessor()
        stock_count = stock_count_processor.get_stock_count(2330, 2018)
        # free_cash_flow_per_share = [cf / stock_count * 1000 for cf in data_frame['業主盈餘現金流']]
        # print(free_cash_flow_per_share)
        data_frame = data_frame.assign(每股業主盈餘現金流=pd.Series([cf / stock_count * 1000 for cf in data_frame['業主盈餘現金流']]).values)
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))


    def test_stock_count(self):
        stock_count_processor = StockCountProcessor()
        stock_count = stock_count_processor.get_stock_count(2330, 2018)
        print(stock_count)

    def test_roe(self):
        roe_utils.get_in_season(2330, 2018, 2)
        pass

    def test_get_matrix_level(self):
        pass

