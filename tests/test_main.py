import unittest

import pandas as pd
import requests
from tabulate import tabulate
from twstock import Stock

import roe_utils
from evaluation_utils import get_matrix_level, get_cash_flow_per_share, get_predict_evaluate, \
    create_stock_datas, get_stock_codes, sync_data, get_cash_flow_per_share_recent, \
    get_stock_data, generate_predictions, get_matrix_value, generate_predictions2
from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.fetcher import DataFetcher
from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from tsec.crawl import Crawler
from twse_crawler import gen_output_path
from utils import get_recent_seasons
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
        # data_frame = income_statement_processor.get_data_frame(2018, 2)
        # self.assertIsNotNone(data_frame)
        # self.assertTrue(data_frame.loc['2018Q2', 'EPS'] is not None)
        # self.assertTrue(data_frame.loc['2018Q2', '稅後淨利'] is not None)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        # data_frame = income_statement_processor.get_data_frames(since={'year': 2017, 'season': 1},
        #                                                         to={'year': 2018, 'season': 2})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        data_frame = income_statement_processor.get_data_frames(since={'year': 2017})
        print(data_frame)

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

        # data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2017})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2017, 'season': 2})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        # data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2018, 'season': 3})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_request_balance_sheet(self):
        balance_sheet_processor = SimpleBalanceSheetProcessor(2330)
        # data_frame = balance_sheet_processor.get_data_frame(2018, 2)
        # self.assertIsNotNone(data_frame)
        # self.assertTrue(data_frame.loc['2018Q2', '每股淨值'] is not None)
        #
        data_frame = balance_sheet_processor.get_data_frames(since={'year': 2016})
        print(data_frame)
        # print(data_frame.loc["2016Q1", '每股淨值'])
        print(data_frame.iloc[[-1, -8],].loc[:, '每股淨值'].sum())
        #
        # data_frame = balance_sheet_processor.get_data_frames(since={'year': 2019})
        # print(data_frame)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_dividend_policy(self):
        dividend_policy_processor = DividendPolicyProcessor(6294)
        # data_frame = dividend_policy_processor.get_data_frame(2017, None)
        data_frame = dividend_policy_processor.get_data_frames({'year': 2017}, None)
        self.assertIsNotNone(data_frame)
        self.assertTrue(data_frame.loc[:, ['現金股利']] is not None)
        self.assertTrue(data_frame.loc[:, ['股息']] is not None)
        print(data_frame)

    def test_price_measurement(self):
        price_measurement_processor = PriceMeasurementProcessor(2330)
        df = price_measurement_processor.get_data_frame()
        self.assertIsNotNone(df)
        print(df.loc['2019', '平均股價'], type(df.loc['2019', '平均股價']))
        self.assertTrue(df.loc[:, ['平均股價']] is not None)

    def test_cash_flow_statement(self):
        cash_flow_processor = CashFlowStatementProcessor(2330)
        # data_frame = cash_flow_processor.get_data_frame(2017, 2)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        # data_frame = cash_flow_processor.get_data_frames(since={"year": 2016, "season": 2})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        data_frame = cash_flow_processor.get_data_frames(since={"year": 2013})
        print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_cash_flow_per_share(self):
        get_cash_flow_per_share(2330, {'year': 2018})

    def test_stock_count(self):
        stock_count_processor = StockCountProcessor()
        stock_count = stock_count_processor.get_stock_count(2330, 2018)
        print(stock_count)

    def test_roe(self):
        roe_utils.get_roe_in_season(2330, 2018, 4)
        # roe_utils.get_recent_four_season(2330)
        # roe_utils.get_roe_in_year(2330, 2017)
        # roe_utils.get_in_year(2330, 2017)
        # roe_utils.get_roe_in_year(2330, 2019)

    def test_get_matrix_level(self):
        matrix_level = get_matrix_level(3431, 2013)
        print('matrix_level = ', matrix_level)

    def test_cash_flow_per_share_recent(self):
        get_cash_flow_per_share_recent(1101)

    def test_get_evaluate_performance(self):
        # stock_data = get_evaluate_performance('2330', 2014)
        # path = gen_output_path('data', 'performance_2330.xlsx')
        # stock_data = get_evaluate_performance('1240', 2014)
        # stock_data = get_evaluate_performance('1218', 2014)
        # print(stock_data.df_statement)
        # print(stock_data.df_performance)
        # print(stock_data.df_profit_matrix)
        # from stock_data import store
        # store(stock_data)
        sync_data(1102)
        # sync_data(1104)
        # sync_data(1101)

    def test_predict_evaluation(self):
        from stock_data import read
        stock_data = read('2330')
        self.assertIsNotNone(stock_data)

        df = None
        with open(gen_output_path('data', 'prices.xlsx'), 'rb') as file:
            df = pd.read_excel(file)
            file.close()

        s_2330 = get_predict_evaluate(stock_data, float(df.loc['2330', '收盤價'])).rename('2330')
        result = pd.concat([s_2330], axis=1)
        print('result 1', result.T)

        stock_data = read('6294')
        s_6294 = get_predict_evaluate(stock_data, float(df.loc['6294', '收盤價'])).rename('6294')
        self.assertIsNotNone(stock_data)

        result.loc[:, '6294'] = s_6294
        print('result 2', result.T)

        result.loc[:, '6294'] = s_6294
        print('result 3', result.T)

    def test_get_matrix_value(self):
        from stock_data import read
        stock_data = read('1102')
        get_matrix_value(stock_data)


    def test_read_stock_data(self):
        from stock_data import read
        stock_data = read('1101')
        self.assertIsNotNone(stock_data)
        print(stock_data.stock_id)
        print(stock_data.df_statement)
        print(stock_data.df_performance)
        print(stock_data.df_profit_matrix)


    def test_generate_time_lines(self):
        self.assertEqual(len(get_recent_seasons(0)), 0)
        self.assertEqual(len(get_recent_seasons(1)), 1)
        self.assertEqual(len(get_recent_seasons(2)), 2)
        self.assertEqual(len(get_recent_seasons(3)), 3)
        self.assertEqual(len(get_recent_seasons(4)), 4)

    def test_get_stock_data(self):
        stock_data = get_stock_data(1102, True)
        self.assertIsNotNone(stock_data)

    def test_integrate(self):
        # generate_predictions(['1470'])
        # generate_predictions(get_stock_codes(stock_type='上市'))
        # create_stock_datas([2330, 6294])
        # create_stock_datas(get_stock_codes(stock_type='上市'))
        # create_stock_datas(get_stock_codes(stock_type='上櫃'))
        # create_profit_matrix(['3232'])
        # create_profit_matrix(get_stock_codes(stock_type='上櫃'))
        # create_profit_matrix(get_stock_codes(stock_type='上櫃'))
        # stock_data = get_stock_data(6294, True)
        # s_prediction = get_predict_evaluate(stock_data)
        generate_predictions([1102])
        # print('prediction = ', s_prediction)
        # stock = Stock('1445')
        # print(stock.price)

    def test_tsec_crawler(self):
        crawler = Crawler()
        df = crawler.get_data((2019, 8, 5))
        with pd.ExcelWriter(gen_output_path('data', 'prices.xlsx')) as writer:
            df.to_excel(writer)
            writer.close()

    def test_get_prediction(self):
        df = None
        with open(gen_output_path('data', 'prices.xlsx'), 'rb') as file:
            df = pd.read_excel(file)
            file.close()
        prices = df.loc[:, '收盤價']
        generate_predictions2(prices, ['2330', '6294'])
