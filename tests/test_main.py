import unittest
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tabulate import tabulate

import roe_utils
from evaluation_utils import get_matrix_level, get_cash_flow_per_share, get_predict_evaluate, \
    create_stock_datas, get_stock_codes, sync_data, get_cash_flow_per_share_recent, \
    get_stock_data, get_matrix_value, generate_predictions2, resync_for_dividend_policy, \
    _sync_performance, _sync_statement, get_stock_list
from evaluation_utils2 import _sync_statements, _sync_profit_statement, _sync_balance_sheet, _sync_cash_flow_statement, \
    _sync_dividend_policy, sync_statements, _read_df_datas, generate_prediction, sync_performance, generate_predictions, \
    Option
from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.dividend_policy2 import DividendPolicyProcessor2
from rdss.fetch_data_utils import fetch_simple_balance_sheet_raw_data, fetch_simple_balance_sheet_raw_datas, \
    fetch_balance_sheet_raw_data, fetch_balance_sheet_raw_datas, fetch_shareholder_equity_raw_data
from rdss.fetcher import DataFetcher
from rdss.statement_fetchers import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from stock_data import read, read_dfs, store_df
from tsec.crawl import Crawler
from twse_crawler import gen_output_path
from utils import get_recent_seasons, get_time_lines
from value_measurement import PriceMeasurementProcessor, IndexType


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
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        data_frame2 = ((data_frame['權益總額']['期初餘額'] + data_frame['權益總額']['期末餘額']) / 2)
        # print(data_frame['權益總額']['期末餘額'])
        print("result = ", data_frame2)

        # data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2018, 'season': 3})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    def test_request_balance_sheet(self):
        balance_sheet_processor = SimpleBalanceSheetProcessor(2330)
        # data_frame = balance_sheet_processor.get_data_frame(2018, 2)
        # self.assertIsNotNone(data_frame)
        # self.assertTrue(data_frame.loc['2018Q2', '每股淨值'] is not None)
        #
        # data_frame = balance_sheet_processor.get_data_frames(since={'year': 2016})
        # data_frame = balance_sheet_processor.get_data_frames(since={'year': 2016, 'season': 1},
        #                                                      to={'year': 2016, 'season': 1})
        data_frame = balance_sheet_processor.get_data_frame(2020, 1)
        print('balance_sheet = ', data_frame)

    def test_get_raw_data(self):
        # stock_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
        stock_list = [2809, 2812, 2816, 2820, 2823, 2832, 2834, 2836, 2838, 2841, 2845, 2849, 2850, 2851, 2852, 2855, 2867, 2880, 2881, 2882, 2883, 2884, 2885, 2886, 2887, 2888, 2889, 2890, 2891, 2892, 2897, 5871, 5876, 5880, 5820, 5864, 5878]
        error_ids = []
        for stock_id in stock_list:
            balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
            raw_data = balance_sheet_processor.get_balance_sheet_raw_data(year=2020, season=1)
            if raw_data is not None:
                # self.store_raw_data(raw_data, 'raw_datas/test_balance_sheets', "balance_sheet_data_" + str(stock_id))
                pass
            else:
                error_ids.append(stock_id)
        print('error_ids = ', error_ids)

    def test_parse_balance_sheet(self):
        # stock_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
        stock_list = [2801]
        # stock_list = [1409]
        empty_list = []
        non_investment = []
        non_property = []
        for stock_id in stock_list:
            # print('parse ', stock_id)
            file_name = "balance_sheet_data_" + str(stock_id)
            input_path = gen_output_path('raw_datas/test_balance_sheets', file_name)
            year = 2020
            season = 1
            balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
            with open(input_path, 'rb') as in_put:
                # str_trs = in_put.readlines()
                raw_input = in_put.read()
                in_put.close()
                soup = BeautifulSoup(raw_input, 'html.parser')
                # print(soup.prettify())
                parse_data = balance_sheet_processor.parse_balance_sheet(soup, year, season)
                if parse_data is not None and len(parse_data) < 2:
                    if len(parse_data) == 0:
                        empty_list.append(stock_id)
                    else:
                        if '長期投資' not in parse_data:
                            non_investment.append(stock_id)
                        if '固定資產' not in parse_data:
                            non_property.append(stock_id)
        print('empty_list = ', empty_list)
        print('non_investment = ', non_investment)
        print('non_property = ', non_property)

    def test_dividend_policy(self):
        dividend_policy_processor = DividendPolicyProcessor(6294)
        # data_frame = dividend_policy_processor.get_data_frame(2017, None)
        data_frame = dividend_policy_processor.get_data_frames({'year': 2017}, None)
        self.assertIsNotNone(data_frame)
        self.assertTrue(data_frame.loc[:, ['現金股利']] is not None)
        self.assertTrue(data_frame.loc[:, ['股息']] is not None)
        print(data_frame)

    def test_sync_dividend_policy(self):
        _sync_dividend_policy(2013, str(2330))
        # dividend_policy_processor = DividendPolicyProcessor2()
        # df_dividend_policy = dividend_policy_processor.get_data_frames(stock_id=str(2330), start_year=2012)

    def test_get_dividend_policy2_raw_data(self):
        dividend_policy_processor = DividendPolicyProcessor2()
        stock_list = get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃')
        for stock_id in stock_list:
            raw_data = dividend_policy_processor._get_raw_data(stock_id, 2013, 2020)
            assert(raw_data is not None)
            self.store_raw_data(raw_data, 'raw_datas/dividend_policies', "dividend_policy_" + str(stock_id))

    def test_parse_dividend_policy2_raw_data(self):
        # raw_data = self.get_raw_data('raw_datas/dividend_policies', "dividend_policy_" + str(3226))
        # dividend_policy_processor = DividendPolicyProcessor2()
        # dividend_policy_processor._parse_raw_data(str(3226), raw_data)
        stock_list = (get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃'))
        for stock_id in stock_list:
            print('parse ', stock_id)
            raw_data = self.get_raw_data('raw_datas/dividend_policies', "dividend_policy_" + str(stock_id))
            dividend_policy_processor = DividendPolicyProcessor2()
            dividend_policy_processor._parse_raw_data(str(stock_id), raw_data)

    def test_pandas(self):
        dates = [pd.Timestamp('2012-05-01'), pd.Timestamp('2012-05-02'), pd.Timestamp('2012-05-03')]
        periods = [pd.Period('2012Q1'), pd.Period('2012Q2'), pd.Period('2012Q3')]
        print(periods[2].start_time)
        ts = pd.Series(pd.np.random.randn(3), periods)
        print(ts)
        print(ts.index)

    def test_price_measurement(self):
        price_measurement_processor = PriceMeasurementProcessor(2330)
        df = price_measurement_processor.get_data_frame(indexType=IndexType.YEAR_INDEX)
        self.assertIsNotNone(df)
        # print(df.loc['2019', '平均股價'], type(df.loc['2019', '平均股價']))
        # self.assertTrue(df.loc[:, ['平均股價']] is not None)
        # print(df)

    def test_cash_flow_statement(self):
        cash_flow_processor = CashFlowStatementProcessor(2330)
        # data_frame = cash_flow_processor.get_data_frame(2017, 2)
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

        # data_frame = cash_flow_processor.get_data_frames(since={"year": 2016, "season": 2})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        data_frame = cash_flow_processor.get_data_frames(since={"year": 2013})
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))
        print(data_frame)

    def test_cash_flow_per_share(self):
        get_cash_flow_per_share(2330, {'year': 2018})

    def test_stock_count(self):
        stock_count_processor = StockCountProcessor()
        # stock_count = stock_count_processor.get_stock_count(2330, 2018)
        # stock_count = stock_count_processor.get_stock_count(6592, 2018)
        # print(stock_count)
        df_stock_count = stock_count_processor.get_data_frame(2330, 2014)

        # df_stock_count = stock_count_processor.get_data_frame(6592, 2014)
        print(df_stock_count)

    def test_roe(self):
        # roe_utils.get_roe_in_season(2330, 2018, 4)
        # roe_utils.get_recent_four_season(2330)
        # roe_utils.get_roe_in_year(2330, 2017)
        roe_utils.get_roe_in_year(2330, 2017)
        # roe_utils.get_roe_in_year(2330, 2019)
        # roe_utils.get_predict_roe_by_relative(1213)
        # roe_utils.get_predict_roe_by_relative(1101)
        # roe_utils.get_predict_roe_by_relative(1413)
        # roe_utils.get_predict_roe_by_relative(2475)

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
        sync_data(1101)
        # sync_data(1104)
        # sync_data(1101)

    def test_sync_performance(self):
        stock_data = read(str(1101))
        _sync_performance(1101, stock_data.df_statement)

    def test_sync_performance2(self):
        # _sync_statements(8013)
        # _sync_profit_statement(2013, 1101)
        # df_balance_sheet = _sync_balance_sheet(2017, 2492)
        # print('column = ', df_balance_sheet.columns)
        # print('column values = ', df_balance_sheet.columns.values)
        # print('column type = ', type(df_balance_sheet.columns))
        # _sync_cash_flow_statement(2013, 1101)
        # _sync_dividend_policy(2013, 1101)
        from evaluation_utils2 import _sync_performance

        # _sync_performance(8103)
        # _sync_performance(1340)
        # df_performance = _sync_performance(2492)
        # generate_prediction(1340, float(8.6))
        # get_stock_codes(stock_type='上市')
        # df_statements['profit_statement'] = _sync_profit_statement(2013, 2492, df_statements.get('profit_statement', None))
        # store_df(2492, df_statements, filename='statments_2492.xlsx')

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
        # create_stock_datas([1213])
        # create_stock_datas(get_stock_codes(stock_type='上市'))
        # create_stock_datas(get_stock_codes(stock_type='上櫃'))
        # get_stock_codes(stock_type='上市', from_item=1413)
        # create_profit_matrix(['3232'])
        # create_profit_matrix(get_stock_codes(stock_type='上櫃'))
        # create_profit_matrix(get_stock_codes(stock_type='上櫃'))
        # stock_data = get_stock_data(6294, True)
        # s_prediction = get_predict_evaluate(stock_data)
        # generate_predictions([1102])
        # print('prediction = ', s_prediction)
        # stock = Stock('1445')
        # print(stock.price)

        # generate_predictions([1101])
        # generate_predictions(get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃'))
        # print('prediction = ', s_prediction)
        stock_code_list = get_stock_codes(stock_type='上市')
        stock_code_list.extend(get_stock_codes(stock_type='上櫃'))
        # print('stock_code_list = ',stock_code_list)
        dr_list = [1262, 9103, 910322, 910482, 9105, 910708, 910861, 9110, 911608, 911616, 911619, 911622, 911868,
                   912000,
                   912398, 9136, 9157, 9188, 911613]
        stock_code_list = list(filter(lambda stock_code: stock_code not in dr_list, stock_code_list))
        print('stock_code = ', stock_code_list)
        # print('index of 1566 = ', stock_code_list.index(1566))  # 1566, 1787
        # print('sub_list = ', stock_code_list[250:])
        # sync_statements(get_stock_codes(stock_type='上市'))
        # sync_statements(get_stock_codes(stock_type='上櫃'))
        sync_statements(stock_code_list, times_to_retry=2, break_after_retry= False, option=Option.DIVIDEND_POLICY, isSync=False)
        # _sync_statements(stock_id=3556, option=Option.BALANCE_SHEET, isSync=False)
        # print((Option.BALANCE_SHEET & Option.DIVIDEND_POLICY) > 0)

    def test_resync(self):
        # error_ids = [1341, 1417, 1587, 1592, 1598, 1760, 1776, 2069, 2236, 2243, 2475, 2630, 2633, 2634, 2739, 2841,
        #              2880, 2881, 2882, 2883, 2884, 2885, 2886, 2887, 2888, 2889, 2890, 2891, 2892, 2897, 2936, 2939,
        #              3321, 3346, 3413, 3519, 3530, 3579, 3711, 3712, 4155, 4190, 4438, 4540, 4545, 4551, 4557, 4560,
        #              4562, 4564, 4566, 4763, 4764, 4766, 4807, 4943, 4961, 4967, 4968, 4989, 5258, 5284, 5288, 5871,
        #              5876, 5880, 6288, 6416, 6431, 6443, 6449, 6451, 6464, 6477, 6531, 6533, 6541, 6558, 6573, 6579,
        #              6581, 6591, 6625, 6641, 6655, 6666, 6668, 6669, 6670, 6671, 6672, 6674, 8028, 8104, 8222, 8341,
        #              8367, 8442, 8454, 8462, 8464, 8466, 8473, 8478, 8480, 8481, 8482, 8488, 8497, 8499, 1240, 1264,
        #              1268, 1566, 1787, 2064, 2065, 2070, 2235, 2643, 2726, 2732, 2736, 2745, 2937, 3066, 3081, 3122,
        #              3147, 3178, 3207, 3306, 3322, 3374, 3426, 3431, 3492, 3562, 3581, 3615, 3672, 4116, 4147, 4167,
        #              4174, 4180, 4183, 4188, 4192, 4198, 4538, 4543, 4550, 4554, 4556, 4561, 4563, 4568, 4714, 4741,
        #              4744, 4754, 4760, 4767, 4806, 4911, 4939, 5220, 5223, 5245, 5291, 5299, 5317, 5543, 5820, 5864,
        #              5878, 6020, 6026, 6170, 6222, 6417, 6418, 6425, 6432, 6435, 6438, 6441, 6446, 6457, 6461, 6462,
        #              6465, 6469, 6472, 6482, 6486, 6492, 6494, 6496, 6497, 6499, 6510, 6512, 6523, 6530, 6532, 6542,
        #              6547, 6556, 6560, 6561, 6568, 6569, 6570, 6574, 6576, 6577, 6578, 6590, 6612, 6613, 6615, 6640,
        #              6654, 6662, 6667, 7402, 8027, 8279, 8342, 8415, 8431, 8437, 8440, 8444, 8455, 8472, 8476, 8477,
        #              8489, 8913, 8928]
        error_ids = [3556]
        # sync_statements(error_ids, 0, False)
        error_ids_after = sync_performance(error_ids)
        print("sync_performance error_ids = ", error_ids_after)
        print('count of errors before = ', len(error_ids), ' after = ', len(error_ids_after))

    def test_sync(self):
        # sync_statements([2330])
        # stock_id = 2330
        # df_statements = _read_df_datas(stock_id)
        # before = df_statements['dividend_policy']
        # df_statements['dividend_policy'] = _sync_dividend_policy(2013, stock_id, df_statements.get('dividend_policy', None))
        # print('before = ', before)
        # print('after = ', df_statements['dividend_policy'])
        # before = df_statements['profit_statement']
        # df_statements['profit_statement'] = _sync_profit_statement(2013, stock_id, df_statements.get('profit_statement', None))
        # print('before = ', before)
        # print('after = ', df_statements['profit_statement'])
        df_stock_list = get_stock_list(stock_type='上市')
        df_stock_list['公司代號'] = df_stock_list['公司代號'].map(lambda stock_id: str(stock_id))
        df_stock_list = df_stock_list.set_index('公司代號')
        print(df_stock_list.loc['2330', :])

    def test_tsec_crawler(self):
        crawler = Crawler()
        df = crawler.get_data((2020, 5, 8))
        with pd.ExcelWriter(gen_output_path('data', 'prices.xlsx')) as writer:
            df.to_excel(writer)
            writer.close()

    def test_get_prediction(self):
        with open(gen_output_path('data', 'prices.xlsx'), 'rb') as file:
            df = pd.read_excel(file)
            file.close()
        prices = df.loc[:, '收盤價']
        errors = generate_predictions(prices, get_stock_codes(stock_type='上市') + get_stock_codes(stock_type='上櫃'))
        # generate_predictions2(prices, [2474, 1227, 2105, 2327, 3213, 4974, 6294, 8066, 8103, 8210, 8416, 8905, 9927])
        print('test_get_prediction errors = ', errors)

    def test_re_sync_dividend(self):
        resync_for_dividend_policy([1102])

    def test_sync_statement(self):
        from evaluation_utils2 import _sync_statements
        statement = _sync_statements(2330)
        # print(statement)

    def test_fetch_data_utils(self):
        # get_simple_balance_sheet_raw_datas([2330], time_lines=get_time_lines(since={'year': 2020}))
        # stock_code_list = get_stock_codes(stock_type='上市')
        # stock_code_list.extend(get_stock_codes(stock_type='上櫃'))
        # get_simple_balance_sheet_raw_data(2330, 2020, 3)
        # get_balance_sheet_raw_datas(stock_code_list)
        fetch_shareholder_equity_raw_data(2330, 2020, 4)


    def store_raw_data(self, data, output_dir, file_name):
        if data is not None:
            output_path = gen_output_path(output_dir, file_name)
            with open(output_path, 'wb') as output:
                output.write(data)
                output.close()

    def get_raw_data(self, input_dir, file_name):
        input_path = gen_output_path(input_dir, file_name)
        with open(input_path, 'rb') as in_put:
            raw_input = in_put.read()
            in_put.close()
            return raw_input
