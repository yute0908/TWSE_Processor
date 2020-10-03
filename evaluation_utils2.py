import enum
import math
import sys
import time
import traceback
from datetime import datetime

import pandas as pd

from evaluation_utils import get_stock_list
from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.dividend_policy2 import DividendPolicyProcessor2
from rdss.statement_fetchers import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from stock_data import store_df, read_dfs
from twse_crawler import gen_output_path
from utils import get_time_lines
from value_measurement import PriceMeasurementProcessor, IndexType


class Option(enum.IntEnum):
    BALANCE_SHEET = 1
    PROFIT_STATEMENT = 1 << 2
    CASH_FLOW_STATEMENT = 1 << 3
    DIVIDEND_POLICY = 1 << 4
    ALL = BALANCE_SHEET | PROFIT_STATEMENT | CASH_FLOW_STATEMENT | DIVIDEND_POLICY


def sync_statements(stock_codes, times_to_retry=10, break_after_retry=True, option=Option.ALL, isSync=True):
    print('stock_codes = ', stock_codes)
    retry = 0
    for stock_code in stock_codes:
        get_data = False
        while get_data is False:
            try:
                _sync_statements(stock_id=stock_code, option=option, isSync=isSync)
                get_data = True
                retry = 0
            except Exception as e:
                retry += 1
                print("get exception", e)
                traceback.print_tb(e.__traceback__)
                if retry >= times_to_retry:
                    print("retry for ", retry, " times to get stock ", stock_code)

                    if break_after_retry:
                        exit(-1)
                    get_data = True
                else:
                    time.sleep(60 * 10)


def sync_performance(stock_codes):
    error_ids = []
    for stock_id in stock_codes:
        print('process ', stock_id)
        try:
            _sync_performance(stock_id)
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            error_ids.append(stock_id)
    return error_ids


def generate_predictions(df_prices, stock_ids=[]):
    error_stock_ids = []
    results = None
    df_predictions = None

    df_stock_list = _get_stock_list()
    for stock_id in stock_ids:
        str_stock_id = str(stock_id)
        try:
            result = generate_prediction(stock_id, float(df_prices.loc[str_stock_id]))
        except Exception as e:
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_tb(e.__traceback__)
            error_stock_ids.append(stock_id)
            result = None

        if result is not None:
            if results is None:
                results = {'公司代號': [stock_id], '公司簡稱': [df_stock_list.loc[str_stock_id, '公司簡稱']]}
                results.update(dict(map(lambda x: (x[0], [x[1]]), result.items())))

                print(list(results.keys()))
                # df_predictions = pd.DataFrame(result)
                # df_predictions['股號'] = [str_stock_id]
                # df_predictions = df_predictions.set_index('股號')
                print("first record")
                # print(df_predictions)
            else:
                for pair in result.items():
                    results[pair[0]].append(pair[1])
                results['公司代號'].append(stock_id)
                results['公司簡稱'].append(df_stock_list.loc[str_stock_id, '公司簡稱'])
                print('results = ', results)

    df_predictions = pd.DataFrame(results)
    df_predictions = df_predictions.set_index('公司代號')
    print("result = ", df_predictions)
    output_path = gen_output_path('data', 'evaluations.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        df_predictions.to_excel(writer, sheet_name='predictions')
    return error_stock_ids


def _get_stock_list():
    df_stock_list = get_stock_list(stock_type='上市').append(get_stock_list(stock_type='上櫃'))
    df_stock_list['公司代號'] = df_stock_list['公司代號'].map(lambda stock_id: str(stock_id))
    df_stock_list = df_stock_list.set_index('公司代號')
    return df_stock_list


def generate_prediction(stock_id, price):
    df_statements = _read_df_datas(stock_id)
    df_balance_sheet = _normalize_balance_sheet(df_statements)
    df_dividend_policy = df_statements['dividend_policy']
    df_performance = df_statements['performance']
    df_profit_statement = df_statements['profit_statement']
    df_cash_flow_statement = df_statements['cash_flow_statement']
    print('cash_flow_first_8q = ', df_statements['cash_flow_statement'].iloc[0: 8])
    indexes = list(filter(lambda period: period.year == 2019, df_balance_sheet.index.values.tolist()))
    year_last_has_dividend = df_dividend_policy.index.values.tolist()[-1]
    year_last_has_dividend_int = year_last_has_dividend.year
    dividend_indexes = list(
        filter(lambda period: period.year == year_last_has_dividend_int, df_dividend_policy.index.values.tolist()))
    next_year_indexes = list(filter(lambda period: period.year == (year_last_has_dividend_int + 1),
                                    df_profit_statement.index.values.tolist()))
    count_of_next_year_indexes = len(next_year_indexes)
    eps_last_year = df_profit_statement.loc[
                    '{}Q1'.format(year_last_has_dividend_int): '{}Q4'.format(year_last_has_dividend_int), 'EPS'].sum()
    eps_last_year_current_q = df_profit_statement.loc[
                              '{}Q1'.format(year_last_has_dividend_int): '{}Q{}'.format(year_last_has_dividend_int,
                                                                                        count_of_next_year_indexes),
                              'EPS'].sum()
    eps_next_year_current_q = df_profit_statement.loc[
                              '{}Q1'.format(year_last_has_dividend_int + 1): '{}Q{}'.format(
                                  year_last_has_dividend_int + 1,
                                  count_of_next_year_indexes),
                              'EPS'].sum()

    predict_eps_next_year = eps_next_year_current_q if (count_of_next_year_indexes == 4) else (
                                                                                                      eps_last_year / eps_last_year_current_q) * eps_next_year_current_q
    predict_dividend_next_year = predict_eps_next_year * df_performance.loc[str(year_last_has_dividend), '兩年現金股利發放率']
    predict_sum_eps_two_year = predict_eps_next_year + eps_last_year
    predict_sum_dividend_two_year = predict_dividend_next_year + df_dividend_policy.loc[year_last_has_dividend, '現金股利']
    predict_dividend_ratio_two_years = predict_sum_dividend_two_year / predict_sum_eps_two_year
    df_balance_subset = df_balance_sheet.loc["{}Q{}".format(year_last_has_dividend_int, 1):]
    predict_avg_roe_two_years = predict_sum_eps_two_year / (
            df_balance_subset.iloc[0].loc['每股淨值'] + df_balance_subset.iloc[-1].loc['每股淨值'])
    predict_pe = price / predict_eps_next_year
    predict_growth_ratio_of_eps = predict_avg_roe_two_years * (1 - predict_dividend_ratio_two_years)
    predict_dividend_ratio = predict_dividend_next_year / price
    peter_lynch_value = (
                                predict_dividend_ratio + predict_growth_ratio_of_eps) / predict_pe * 100 if predict_pe > 0 else 0

    predict_cash_flow_per_share = df_cash_flow_statement.iloc[0: 8 if (count_of_next_year_indexes == 4) else 4].loc[:,
                                  '業主盈餘現金流'].sum() / df_dividend_policy.iloc[-1].loc['股數']
    matrix_levels = [_get_matrix_level(predict_avg_roe_two_years, predict_cash_flow_per_share)] + df_performance.loc[:,
                                                                                                  '矩陣等級'].values.tolist()[
                                                                                                  ::-1]
    print('matrix_levels = ', matrix_levels)

    def peter_lynch_reverse(val):
        value = math.pow(predict_growth_ratio_of_eps, 2) + 4 * predict_dividend_next_year * (
                val / 100) / predict_eps_next_year
        return 0 if value <= 0 else (predict_growth_ratio_of_eps + math.sqrt(value)) / (
                2 * (val / 100) / predict_eps_next_year)

    result = {'股價': price, '本益比': predict_pe, '彼得林區評價': peter_lynch_value,
              '彼得林區評價2倍股價': peter_lynch_reverse(2.0),
              '彼得林區評價1.5倍股價': peter_lynch_reverse(1.5), '彼得林區評價1倍股價': peter_lynch_reverse(1.0),
              '預估現金股利': predict_dividend_next_year, '預估現金殖利率': predict_dividend_ratio,
              '預估ROE': predict_avg_roe_two_years, '預估業主盈餘現金流': predict_cash_flow_per_share}
    result.update(_transform_matrix_value(matrix_levels))

    print('last_year_has_dividend = ', year_last_has_dividend)
    print('eps_last_year = ', eps_last_year)
    print('predict_eps_next_year = ', predict_eps_next_year)
    print('predict_dividend_next_year = ', predict_dividend_next_year)
    print('predict_dividend_ratio_two_years = ', predict_dividend_ratio_two_years)
    print('predict_avg_roe_two_years = ', predict_avg_roe_two_years)
    print('predict_growth_ratio_of_eps = ', predict_growth_ratio_of_eps)
    print('predict_pe = ', predict_pe)
    print('predict_cash_flow_per_share = ', predict_cash_flow_per_share)
    print('peter_lynch_value = ', peter_lynch_value)
    print('result = ', result)
    return result


matrix_value_dic = {"A": 12, "B1": 8, "B2": 6, "C": 4, "C1": 2, "C2": 1, "D": 0}


def _transform_matrix_value(matrix_levels=None):
    if matrix_levels is None or len(matrix_levels) == 0:
        return None

    weight = [1, 0.8, 0.5, math.pow(0.5, 2), math.pow(0.5, 3), math.pow(0.5, 4)]
    use_count = min(len(matrix_levels), len(weight))
    matrix_str = ",".join([matrix_levels[0], "[" + ",".join(matrix_levels[1: use_count + 1]) + "]"])
    result = sum([matrix_value_dic[matrix_levels[i]] * weight[i] for i in range(0, use_count)])
    return {'矩陣等級': matrix_str, '矩陣分數': result}


def _sync_performance(stock_id):
    df_statements = _read_df_datas(stock_id)
    df_balance_sheet = _normalize_balance_sheet(df_statements)
    df_profit_statement = df_statements['profit_statement']
    df_cash_flow_statement = df_statements['cash_flow_statement']
    df_dividend_policy = df_statements['dividend_policy']
    print('df_profit_statement', df_profit_statement)
    print('df_balance_sheet = ', df_balance_sheet)
    print('df_cash_flow_statement', df_cash_flow_statement)
    print('df_dividend_policy', df_dividend_policy)
    now = datetime.now()
    start_year = max(df_dividend_policy.index.values[0].year + 1, 2014)
    # results_data = []
    # results_index = []
    dfs_result = []
    profits = {}
    fix_assets = {}
    for year in range(start_year, df_dividend_policy.index.values[-1].year + 1):
        df_profit_subset = df_profit_statement.loc["{}Q{}".format(year - 1, 1):"{}Q{}".format(year, 4)]
        df_balance_subset = df_balance_sheet.loc["{}Q{}".format(year - 1, 1): "{}Q{}".format(year, 4)]
        sum_of_eps_two_years = df_profit_subset.loc[:, 'EPS'].sum()
        value_in_two_years = df_balance_subset.iloc[0].loc["每股淨值"] + df_balance_subset.iloc[-1].loc["每股淨值"]
        dividend_in_two_years = df_dividend_policy.loc[str(year - 1): str(year), "現金股利"].sum()
        dividend_ratio_two_years = dividend_in_two_years / sum_of_eps_two_years
        avg_roe_two_years = sum_of_eps_two_years / value_in_two_years
        growth_ratio_of_eps = avg_roe_two_years * (1 - dividend_ratio_two_years)
        first_q = '{}Q{}'.format(year, 1)
        fourth_q = '{}Q{}'.format(year, 4)
        profit = df_profit_statement.loc[first_q: fourth_q, '稅後淨利'].sum()
        profits[year] = profit
        shareholder_equity = ((df_balance_sheet.loc[first_q, '(權益總額, 期初餘額)'] + df_balance_sheet.loc[
            fourth_q, '(權益總額, 期末餘額)']) / 2)
        roe = profit / shareholder_equity

        cash_flow_per_share = df_cash_flow_statement.loc[fourth_q: first_q, '業主盈餘現金流'].sum() / df_dividend_policy.loc[
            str(year), '股數']
        try:
            dividend_yield = dividend_ratio_two_years / 2 / float(df_dividend_policy.loc[str(year), '平均股價'])
        except Exception as e:
            print('get', e, ' when get price for ', ' in ', year)
            dividend_yield = float(0)
        dict_result = {'兩年現金股利發放率': dividend_ratio_two_years, '兩年平均ROE': avg_roe_two_years,
                       '保留盈餘成長率': growth_ratio_of_eps,
                       '現金股利殖利率': dividend_yield, '高登模型預期報酬率': growth_ratio_of_eps + dividend_yield,
                       '兩年平均現金股利': dividend_in_two_years, 'ROE': roe, '每股業主盈餘現金流': cash_flow_per_share,
                       '矩陣等級': _get_matrix_level(roe, cash_flow_per_share)}
        dfs_result.append(pd.DataFrame([dict_result.values()], columns=dict_result.keys(),
                                       index=pd.PeriodIndex(start=pd.Period(year, freq='Y'),
                                                            end=pd.Period(year, freq='Y'), freq='Y')))
        # results_index.append(pd.PeriodIndex(start=pd.Period(year, freq='Y'), end=pd.Period(year, freq='Y'), freq='Y'))
        # results_data.append()
    # print('results_index = ', results_index)
    # print('results_data = ', results_data)
    # df_performance = pd.DataFrame(results_data, index=results_index)
    print('profits = ', profits)
    df_performance = pd.concat(dfs_result, sort=True) if len(dfs_result) > 0 else None
    df_statements['performance'] = df_performance
    store_df(stock_id, df_statements, filename='statments_{0}.xlsx'.format(stock_id))
    return df_performance


def _normalize_balance_sheet(df_statements):
    df_balance_sheet = df_statements['balance_sheet'].copy()
    column_list = df_balance_sheet.columns.tolist()
    column_list[3] = '(權益總額, 期初餘額)'
    column_list[4] = '(權益總額, 期末餘額)'
    df_balance_sheet.columns = column_list
    return df_balance_sheet


def _get_matrix_level(roe, cash_flow_per_share):
    if roe >= 0.15:
        return "A" if cash_flow_per_share > 0 else "B1"
    if roe >= 0.10:
        return "B2" if cash_flow_per_share > 0 else "C"
    if roe > 0:
        return "C1" if cash_flow_per_share > 0 else "C2"
    return "D"


def _sync_statements(stock_id, option=Option.ALL, isSync=True):
    start_year = 2013
    df_statements = _read_df_datas(stock_id)
    df_profit_statement = _sync_profit_statement(start_year, stock_id,
                                                 df_profit_statement=df_statements.get('profit_statement',
                                                                                       None) if isSync else None) \
        if (option & Option.PROFIT_STATEMENT) > 0 else df_statements.get('profit_statement', None)

    df_balance_sheet = _sync_balance_sheet(start_year, stock_id,
                                           df_balance_sheet=df_statements.get('balance_sheet',
                                                                              None) if isSync else None) \
        if (option & Option.BALANCE_SHEET > 0) else df_statements.get('balance_sheet', None)

    df_cash_flow_statement = _sync_cash_flow_statement(start_year, stock_id,
                                                       df_cash_flow_statement=df_statements.get('cash_flow_statement',
                                                                                                None) if isSync else None)\
        if (option & Option.CASH_FLOW_STATEMENT) > 0 else df_statements.get('cash_flow_statement', None)

    df_dividend_policy = _sync_dividend_policy(start_year, stock_id,
                                               df_dividend_policy=df_statements.get('dividend_policy', None) if isSync else None)\
        if (option & Option.DIVIDEND_POLICY) > 0 else df_statements.get('dividend_policy', None)

    print('df_profit_statement = ', df_profit_statement)
    print('df_balance_sheet = ', df_balance_sheet)
    print('df_cash_flow_statement = ', df_cash_flow_statement)
    print('df_dividend_policy = ', df_dividend_policy)
    if df_profit_statement is None or df_balance_sheet is None or df_cash_flow_statement is None or df_dividend_policy \
            is None:
        return None
    store_df(stock_id, {'profit_statement': df_profit_statement,
                        'balance_sheet': df_balance_sheet,
                        'cash_flow_statement': df_cash_flow_statement,
                        'dividend_policy': df_dividend_policy},
             filename='statments_{0}.xlsx'.format(stock_id))


def _sync_cash_flow_statement(start_year, stock_id, df_cash_flow_statement=None):
    cash_flow_processor = CashFlowStatementProcessor(stock_id)
    if df_cash_flow_statement is None:
        df_cash_flow_statement = cash_flow_processor.get_data_frames({'year': start_year - 1})
    else:
        time_lines = get_time_lines(since={'year': start_year})
        dfs_get = []
        index_string_list = df_cash_flow_statement.index.map(str).values.tolist()
        for time_line in time_lines:
            row_index = "{}Q{}".format(time_line['year'], time_line['season'])
            if not (row_index in index_string_list):
                df_statement = cash_flow_processor.get_data_frame(time_line['year'], time_line['season'])
                if df_statement is not None:
                    dfs_get.append(df_statement)

        if len(dfs_get) > 0:
            dfs_get.append(df_cash_flow_statement)
            df_cash_flow_statement = pd.concat(dfs_get, sort=False).sort_index()

    return df_cash_flow_statement


def _sync_balance_sheet(start_year, stock_id, df_balance_sheet=None):
    balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
    shareholder_equity_processor = ShareholderEquityProcessor(stock_id)
    if df_balance_sheet is not None:
        time_lines = get_time_lines(since={'year': start_year})
        dfs_get = []
        for time_line in time_lines:

            row_index = "{}Q{}".format(time_line['year'], time_line['season'])
            val = df_balance_sheet.get(row_index, None)

            is_empty = val is None or len(val.values) == 0
            if is_empty:
                df_balance = balance_sheet_processor.get_data_frame(time_line['year'], time_line['season'])
                df_shareholder = shareholder_equity_processor.get_data_frame(time_line['year'], time_line['season'])
                df_combine = None if df_balance is None or df_shareholder is None else df_balance.join(df_shareholder,
                                                                                                       how='outer')
                if df_combine is not None:
                    dfs_get.append(df_combine)
        if len(dfs_get) > 0:
            dfs_get.append(df_balance_sheet)
            df_balance_sheet = pd.concat(dfs_get, sort=False).sort_index()
        return df_balance_sheet
    else:
        df_balance_statement = balance_sheet_processor.get_data_frames({'year': start_year - 1})
        df_shareholder_equity = shareholder_equity_processor.get_data_frames({'year': start_year - 1})
        if df_balance_statement is None or df_shareholder_equity is None:
            return None
        df_combine = df_balance_statement.join(df_shareholder_equity, how='outer')
        indexes = df_combine[df_combine['每股淨值'].isna()].index
        df_combine.drop(indexes, inplace=True)
        print('合併 = ', df_combine)
        return df_combine


def _sync_profit_statement(start_year, stock_id, df_profit_statement=None):
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    if df_profit_statement is None:
        df_profit_statement = income_statement_processor.get_data_frames({'year': start_year - 1})
    else:
        time_lines = get_time_lines(since={'year': start_year})
        dfs_get = []
        for time_line in time_lines:
            row_index = "{}Q{}".format(time_line['year'], time_line['season'])
            val = df_profit_statement.get(row_index, None)

            is_empty = val is None or len(val.values) == 0
            if is_empty:
                df_statement = income_statement_processor.get_data_frame(time_line['year'], time_line['season'])
                if df_statement is not None:
                    dfs_get.append(df_statement)
        if len(dfs_get) > 0:
            dfs_get.append(df_profit_statement)
            df_profit_statement = pd.concat(dfs_get, sort=False).sort_index()
    return df_profit_statement


def _sync_dividend_policy(start_year, stock_id, df_dividend_policy=None):
    # now = datetime.now()
    dividend_policy_processor = DividendPolicyProcessor2()
    # df_dividend_policy = dividend_policy_processor.get_data_frames({'year': start_year - 1}, {'year': now.year})
    stock_count_processor = StockCountProcessor()
    price_measurement_processor = PriceMeasurementProcessor(stock_id)

    if df_dividend_policy is None:
        # df_dividend_policy = dividend_policy_processor.get_data_frames({'year': start_year - 1})
        df_dividend_policy = dividend_policy_processor.get_data_frames(stock_id=stock_id, start_year=start_year - 1)
        df_stock_count = stock_count_processor.get_data_frame(stock_id, start_year)
        df_prices = price_measurement_processor.get_data_frame(indexType=IndexType.YEAR_INDEX)
        print("stock_counts = ", df_stock_count)
        print('df_dividend_policy = ', df_dividend_policy)
        print('df_prices = ', df_prices)
        if df_stock_count is None or df_dividend_policy is None or df_prices is None:
            return None
        df_combine = df_dividend_policy.join(df_stock_count, how='outer').join(df_prices, how='outer')
        indexes_to_drop = df_combine[df_combine['現金股利'].isna()].index
        df_combine.drop(indexes_to_drop, inplace=True)
        print('合併 = ', df_combine)
        # print('test value ', df_combine.loc['2019']['現金股利'])
        # print('test value ', pd.isna(df_combine.loc['2019']['現金股利']))
        return df_combine
    else:
        # TODO:// implement sync dividend
        return df_dividend_policy


def _read_df_datas(stock_id):
    df_statements = read_dfs(stock_id, gen_output_path('data', 'statments_{0}.xlsx'.format(str(stock_id))))
    if df_statements is None:
        df_statements = {}
    df_profit_statement = df_statements.get('profit_statement', None)
    df_balance_sheet = df_statements.get('balance_sheet', None)
    df_cash_flow_statement = df_statements.get('cash_flow_statement', None)
    df_dividend_policy = df_statements.get('dividend_policy', None)
    df_performance = df_statements.get('performance', None)
    print('df_cash_flow_statement = ', df_cash_flow_statement)
    print('df_dividend_policy = ', df_dividend_policy)
    print('df_balance_sheet = ', df_balance_sheet)

    if df_profit_statement is not None:
        df_profit_statement = df_profit_statement.set_index([df_profit_statement.index.to_period("Q")])
        df_statements['profit_statement'] = df_profit_statement
        print('df_profit_statement = ', df_profit_statement)

    if df_balance_sheet is not None:
        df_balance_sheet = df_balance_sheet.set_index([df_balance_sheet.index.to_period("Q")])
        df_balance_sheet.columns = ['固定資產', '每股淨值', '長期投資', ('權益總額', '期初餘額'), ('權益總額', '期末餘額')]
        df_statements['balance_sheet'] = df_balance_sheet
        print('df_balance_sheet columns = ', df_balance_sheet.columns)

    if df_cash_flow_statement is not None:
        df_cash_flow_statement = df_cash_flow_statement.set_index([df_cash_flow_statement.index.to_period("Q")])
        df_statements['cash_flow_statement'] = df_cash_flow_statement
        print('df_cash_flow_statement = ', df_cash_flow_statement)

    if df_dividend_policy is not None:
        df_dividend_policy = df_dividend_policy.set_index([df_dividend_policy.index.to_period("Y")])
        df_statements['dividend_policy'] = df_dividend_policy
        print('df_dividend_policy = ', df_dividend_policy)

    if df_performance is not None:
        df_performance = df_performance.set_index([df_performance.index.to_period("Y")])
        df_statements['performance'] = df_performance
    return df_statements
