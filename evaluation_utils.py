import sys

import math
import socket
import time
import traceback
from urllib.request import urlopen

import pandas as pd
import requests
import socks
import twstock
from datetime import datetime

from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config
from urllib3.exceptions import NewConnectionError

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from rdss.utils import normalize_params
from roe_utils import get_roe_in_year, get_predict_roe_by_recent_four_season, get_predict_roe_by_relative
from stock_data import StockData, read, store
from twse_crawler import gen_output_path
from utils import get_recent_seasons
from value_measurement import PriceMeasurementProcessor

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
            'SocksPort': str(proxy_port),
            'ControlPort': str(ctrl_port)
        },
        take_ownership=True)


def get_evaluate_performance(stock_id, since_year, to_year=None):
    params = normalize_params(stock_id, since_year, to_year)
    print(params)
    if params is None:
        return None

    stock_id = params.get('stock_id')
    evaluate_since_year = params.get('since_year')
    evaluate_to_year = params.get('to_year')
    get_recent_four_seasons = params.get('get_resent_four_seasons')

    dividend_policy_processor = DividendPolicyProcessor(stock_id)
    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': evaluate_to_year},
                                                                   {'year': evaluate_to_year + 1})

    try:
        should_modify_since_year = False if df_dividend_policy is not None and str(
            evaluate_since_year) in df_dividend_policy.index else True
    except KeyError:
        should_modify_since_year = True
        print('get error')
        evaluate_since_year = evaluate_since_year - 1 if should_modify_since_year and evaluate_since_year >= evaluate_to_year - 1 else evaluate_since_year

    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': evaluate_since_year},
                                                                   {'year': evaluate_to_year + 1})
    df_dividend_policy.sort_index(inplace=True)

    df_income_statement = SimpleIncomeStatementProcessor(stock_id).get_data_frames({'year': evaluate_since_year - 1},
                                                                                   {'year': evaluate_to_year})
    df_income_statement.sort_index(inplace=True)

    balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
    df_balance_sheet = balance_sheet_processor.get_data_frames({'year': evaluate_since_year - 1},
                                                               {'year': evaluate_to_year})

    price_measurement_processor = PriceMeasurementProcessor(stock_id)
    df_prices = price_measurement_processor.get_data_frame()

    results_data = []
    results_index = []
    for year in range(evaluate_since_year, evaluate_to_year + 1):
        count_of_effects = len(df_income_statement.loc["{}Q{}".format(year - 1, 1):"{}Q{}".format(year, 4)].index)
        if count_of_effects == 8:
            sum_of_eps_two_years = \
                df_income_statement.loc["{}Q{}".format(year - 1, 1):"{}Q{}".format(year, 4)].sum().loc["EPS"]
            value_in_two_years = df_balance_sheet.loc["{}Q{}".format(year - 1, 1), "每股淨值"] + df_balance_sheet.loc[
                "{}Q{}".format(year, 4), "每股淨值"]
        else:
            sum_of_eps_two_years = df_income_statement.iloc[-8:].loc[:, "EPS"].sum()
            value_in_two_years = df_balance_sheet.iloc[[-8, -1],].loc[:, "每股淨值"].sum()

        has_dividend = str(year) in df_dividend_policy.index
        predict = not count_of_effects == 8 or not has_dividend
        if has_dividend:
            dr_in_two_years = df_dividend_policy.loc[str(year - 1): str(year), "現金股利"].sum()
            print('dr_in_two_years in ', year, ' = ', dr_in_two_years)
            dividend_ratio_two_years = dr_in_two_years / sum_of_eps_two_years
        else:
            if len(results_data) > 0:
                dividend_ratio_two_years = results_data[-1].get('兩年現金股利發放率')
            else:
                dividend_ratio_two_years = 0
            print('year ', year, '兩年現金股利發放率 = ', dividend_ratio_two_years, ' EPS = ',
                  df_income_statement.iloc[-4:].loc[:, "EPS"].sum())
            dr_in_two_years = dividend_ratio_two_years * df_income_statement.iloc[-4:].loc[:, "EPS"].sum()

        # print('兩年現金股利發放率 in ', year, ':', dividend_ratio_two_years)
        avg_roe_two_years = sum_of_eps_two_years / value_in_two_years
        growth_ratio_of_eps = avg_roe_two_years * (1 - dividend_ratio_two_years)
        try:
            dividend_yield = dr_in_two_years / 2 / float(df_prices.loc[str(year), '平均股價'])
        except Exception as e:
            print('get', e, ' when get price for ', stock_id, ' in ', year)
            dividend_yield = float(0)
        results_index.append(str(year))
        results_data.append(
            {'兩年現金股利發放率': dividend_ratio_two_years, '兩年平均ROE': avg_roe_two_years, '保留盈餘成長率': growth_ratio_of_eps,
             '現金股利殖利率': dividend_yield, '高登模型預期報酬率': growth_ratio_of_eps + dividend_yield, '預估值': predict,
             '現金股利': df_dividend_policy.loc[str(year), "現金股利"] if has_dividend else dr_in_two_years})

    merged_statement = pd.concat([df_income_statement, df_balance_sheet], axis=1, sort=False)
    # print(df_results)
    return StockData(stock_id, pd.DataFrame(results_data, index=results_index), merged_statement,
                     get_matrix_level(stock_id, since_year, to_year))


def sync_data(stock_id):
    stock_data = read(str(stock_id))
    df_statement = _sync_statement(stock_id, None if stock_data is None else stock_data.df_statement)
    df_performance = _sync_performance(stock_id, df_statement, None if stock_data is None else stock_data.df_performance)
    df_profit_matrix = _sync_profit_matrix(stock_id, None if stock_data is None else stock_data.df_profit_matrix)
    return StockData(stock_id, df_performance, df_statement,
                     df_profit_matrix) if df_statement is not None and df_performance is not None and df_profit_matrix is not None else None


def resync_for_dividend_policy(stock_ids):
    for stock_id in stock_ids:
        print('start to resync ', stock_id)
        _resync_dividend_policy(stock_id)


def _resync_dividend_policy(stock_id):
    stock_data = read(str(stock_id))
    if stock_data is None:
        return
    print(stock_data.df_performance.index)
    price_measurement_processor = PriceMeasurementProcessor(stock_id)
    df_prices = price_measurement_processor.get_data_frame()
    # print('index = ', df_prices.index)
    # print('type = ', type(df_prices.index))

    dividend_policy_processor = DividendPolicyProcessor(stock_id)
    year_indexes = stock_data.df_performance.index.tolist()
    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': year_indexes[0]},
                                                                   {'year': year_indexes[-1]})

    prices = []
    shares = []

    for year in stock_data.df_performance.index.tolist():
        prices.append(df_prices.loc[str(year), '平均股價'] if str(year) in df_prices.index else 0)
        shares.append(df_dividend_policy.loc[str(year), '股息'] if str(year) in df_dividend_policy.index else float(0))

    print('prices = ', prices)
    print('result_share = ', shares)
    stock_data.df_performance = stock_data.df_performance.assign(股息=shares).assign(平均股價=prices)
    print('result = ', stock_data.df_performance)
    store(stock_data)


def _sync_performance(stock_id, df_statement, df_performance=None):
    if df_statement is None:
        return None

    results_data = [] if df_performance is None else df_performance.to_dict('records')
    results_index = [] if df_performance is None else df_performance.index.tolist()
    print('results_index = ', results_index)
    print('results_data = ', results_data)
    for index in results_index:
        print('index ', index, ' value = ', str(df_performance.loc[index]['預估值']), ' type = ',
              df_performance.loc[index]['預估值'])
        if df_performance.loc[index]['預估值']:
            df_performance = df_performance.drop(index=index)
    print('after drop df_performance = ', df_performance)
    results_data = [] if df_performance is None else df_performance.to_dict('records')
    results_index = [] if df_performance is None else df_performance.index.tolist()
    print('results_index 2 = ', results_index)
    print('results_data 2 = ', results_data)

    start_year = max(2014, 0 if len(results_index) == 0 else results_index[-1] + 1)
    print('start_year = ', start_year)

    now = datetime.now()
    dividend_policy_processor = DividendPolicyProcessor(stock_id)
    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': start_year - 1}, {'year': now.year})
    print('df_dividend_policy = ', df_dividend_policy)

    price_measurement_processor = PriceMeasurementProcessor(stock_id)
    df_prices = price_measurement_processor.get_data_frame()

    for year in range(start_year, now.year + 1):
        count_of_effects = len(df_statement.loc["{}Q{}".format(year - 1, 1):"{}Q{}".format(year, 4)].index)
        if count_of_effects == 8:
            sum_of_eps_two_years = \
                df_statement.loc["{}Q{}".format(year - 1, 1):"{}Q{}".format(year, 4)].sum().loc["EPS"]
            value_in_two_years = df_statement.loc["{}Q{}".format(year - 1, 1), "每股淨值"] + df_statement.loc[
                "{}Q{}".format(year, 4), "每股淨值"]
        else:
            sum_of_eps_two_years = df_statement.iloc[-8:].loc[:, "EPS"].sum()
            value_in_two_years = df_statement.iloc[[-8, -1], ].loc[:, "每股淨值"].sum()
        has_dividend = str(year) in df_dividend_policy.index
        predict = not count_of_effects == 8 or not has_dividend
        if has_dividend:
            dr_in_two_years = df_dividend_policy.loc[str(year - 1): str(year), "現金股利"].sum()
            dividend_ratio_two_years = dr_in_two_years / sum_of_eps_two_years
        else:
            if len(results_data) > 0:
                dividend_ratio_two_years = results_data[-1].get('兩年現金股利發放率')
            else:
                dividend_ratio_two_years = 0
            dr_in_two_years = dividend_ratio_two_years * df_statement.iloc[-4:].loc[:, "EPS"].sum()

        avg_roe_two_years = sum_of_eps_two_years / value_in_two_years
        growth_ratio_of_eps = avg_roe_two_years * (1 - dividend_ratio_two_years)
        try:
            dividend_yield = dr_in_two_years / 2 / float(
                df_prices.loc[str(year), '平均股價'] if str(year) in df_prices.index else df_prices.iloc[0].get('平均股價'))
        except Exception as e:
            print('get', e, ' when get price for ', stock_id, ' in ', year)
            dividend_yield = float(0)

        results_index.append(str(year))
        results_data.append(
            {'兩年現金股利發放率': dividend_ratio_two_years, '兩年平均ROE': avg_roe_two_years, '保留盈餘成長率': growth_ratio_of_eps,
             '現金股利殖利率': dividend_yield, '高登模型預期報酬率': growth_ratio_of_eps + dividend_yield, '預估值': predict,
             '現金股利': df_dividend_policy.loc[str(year), "現金股利"] if has_dividend else dr_in_two_years})
        df_performance = pd.DataFrame(results_data, index=results_index)
        print('df_performance = ', df_performance)
    return df_performance


def _sync_statement(stock_id, df_statement=None):
    if df_statement is None:
        df_income_statement = SimpleIncomeStatementProcessor(stock_id).get_data_frames({'year': 2013})
        df_income_statement.sort_index(inplace=True)
        balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
        df_balance_sheet = balance_sheet_processor.get_data_frames({'year': 2013})
        shareholder_euity_processor = ShareholderEquityProcessor(2330)
        shareholder_data_frame = shareholder_euity_processor.get_data_frames(since={'year': 2013})
        shareholder_data_frame2 = ((shareholder_data_frame['權益總額']['期初餘額'] + shareholder_data_frame['權益總額']['期末餘額']) / 2)
        result = pd.concat([df_income_statement, df_balance_sheet, shareholder_data_frame2.rename('權益總額')], axis=1, sort=False)
        print('1 result = ', result)
        # print('income statement')

    else:
        now = datetime.now()
        current_quarter = '{}Q{}'.format(now.year, int((now.month - 1) / 3 + 1))
        period_current = pd.Period(current_quarter, freq='Q')
        index = df_statement.index.to_period("Q")
        df_statement = df_statement.reset_index(drop=True).set_index([index])
        print('2 statement = ', df_statement)
        period_last = df_statement.index.values[-1]
        if period_last < period_current:
            start_year = period_last.year if period_last.quarter < 4 else (period_last.year + 1)
            start_quarter = (period_last.quarter + 1) if period_last.quarter < 4 else 1
            df_income_statement = SimpleIncomeStatementProcessor(stock_id).get_data_frames(
                {'year': start_year, 'season': start_quarter},
                {'year': period_current.year, 'season': period_current.quarter})

            balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
            df_balance_sheet = balance_sheet_processor.get_data_frames({'year': start_year, 'season': start_quarter},
                                                                       {'year': period_current.year,
                                                                        'season': period_current.quarter})
            if df_income_statement is not None and df_balance_sheet is not None:
                concat_df = pd.concat([df_income_statement, df_balance_sheet], axis=1, sort=False)
                result = df_statement.append([concat_df])
            else:
                result = df_statement
            print('2 - 1 result = ', result)
        else:
            result = df_statement
    return result


def _sync_profit_matrix(stock_id, df_profit_matrix=None):
    print('df_profit_matrix = ', df_profit_matrix)
    results_index = []
    if df_profit_matrix is not None:
        index = df_profit_matrix.index.to_period('A')
        df_profit_matrix = df_profit_matrix.reset_index(drop=True).set_index([index])
        if '近四季' not in df_profit_matrix.columns:
            df_profit_matrix['近四季'] = False
        # delete_row = df_profit_matrix[df_profit_matrix['近四季'] is False].index
        # print('delete_row = ', delete_row)
        df_profit_matrix = df_profit_matrix[df_profit_matrix['近四季'] == False]
        results_data = df_profit_matrix.to_dict('records')
        results_index = df_profit_matrix.index.year.tolist()
        print('fuck results_data = ', results_data, ' results_index = ', results_index)

    print('df_profit_matrix 2 = ', df_profit_matrix)

    # print('results_data = ', results_data)
    # print('results_index = ', results_index)
    start_year = max(2014, 0 if len(results_index) == 0 else results_index[-1] + 1)
    now = datetime.now()
    dfs = []
    for year in range(start_year, now.year):
        matrix_level = _get_matrix_level_in_year(stock_id, year)
        if matrix_level is not None:
            dfs.append(matrix_level)
        # print('matrix_level in ', year, ' = ', matrix_level)
    df_recent_four_season = _get_matrix_level_in_year(stock_id, now.year, True)
    if df_recent_four_season is not None:
        dfs.append(df_recent_four_season)
    data_frame = None if len(dfs) == 0 else pd.concat(dfs)
    if data_frame is None:
        result = df_profit_matrix
    else:
        result = data_frame if df_profit_matrix is None else pd.concat([df_profit_matrix, data_frame], sort=True)
    print('result = ', result)
    return result


matrix_value_dic = {"A": 12, "B1": 8, "B2": 6, "C": 4, "C1": 2, "C2": 1, "D": 0}


def get_matrix_value(stock_data):
    df_profit_matrix = stock_data.df_profit_matrix
    print('the last record = ', df_profit_matrix.iloc[-1], ' type = ', type(df_profit_matrix.iloc[-1].loc['近四季']))
    # print(matrix_levels)
    matrix_levels = df_profit_matrix['矩陣等級'].values[::-1]

    if df_profit_matrix.iloc[-1].loc['近四季'] == True:
        weight = [1, 0.8, 0.5, math.pow(0.5, 2), math.pow(0.5, 3), math.pow(0.5, 4)]
        use_count = min(len(matrix_levels), len(weight))
        matrix_str = ",".join(
            [matrix_levels[0], "[" + ",".join(matrix_levels[1: use_count + 1]) + "]"])

    else:
        weight = [1, 0.5, math.pow(0.5, 2), math.pow(0.5, 3), math.pow(0.5, 4)]
        use_count = min(len(matrix_levels), len(weight))
        matrix_str = "[" + ",".join(matrix_levels[:]) + "]"

    result = sum([matrix_value_dic[matrix_levels[i]] * weight[i] for i in range(0, use_count)])
    # print([matrix_value_dic[key] for key in matrix_levels[0:use_count]])
    # print(matrix_str)
    # print(result)
    return {'矩陣等級': matrix_str, '矩陣分數':result}


def get_predict_evaluate(stock_data, latest_price):
    # stock = Stock(stock_data.stock_id)

    # print(stock.price)
    eps_last_four_season = stock_data.df_statement.iloc[-4:].loc[:, 'EPS'].sum()
    # latest_price = next((value for value in reversed(stock.price) if value is not None), 0.0)
    # latest_price = next((value for value in reversed(stock.price) if value is not None), 0.0)
    print('stock_price = ', latest_price, ' eps_last_four_season = ', eps_last_four_season)
    predict_pe = latest_price / eps_last_four_season
    g = stock_data.df_performance.iloc[-1].at['保留盈餘成長率']
    y = 0.0 if latest_price <= 0 else stock_data.df_performance.iloc[-1].at['現金股利'] / latest_price
    print('y = ', y, 'g = ', g)
    peter_lynch_value = (y + g) / predict_pe * 100 if predict_pe > 0 else 0
    print('本益比 = ', predict_pe, '彼得林區評價 = ', peter_lynch_value)

    def peter_lynch_reverse(val):
        value = math.pow(g, 2) + 4 * stock_data.df_performance.iloc[-1].at['現金股利'] * (val / 100) / eps_last_four_season

        return 0 if value <= 0 else (g + math.sqrt(value)) / (2 * (val / 100) / eps_last_four_season)

    result = {'股價': latest_price, '本益比': predict_pe, '彼得林區評價': peter_lynch_value,
              '彼得林區評價2倍股價': peter_lynch_reverse(2.0),
              '彼得林區評價1.5倍股價': peter_lynch_reverse(1.5), '彼得林區評價1倍股價': peter_lynch_reverse(1.0)}
    matrix_value = get_matrix_value(stock_data)
    return pd.Series({**result, **matrix_value})

def generate_predictions2(df_prices, stock_ids=[]):
    error_stock_ids = []
    df_predictions = None

    for stock_id in stock_ids:
        str_stock_id = str(stock_id)

        # try:
        #     stock_data = get_stock_data(str_stock_id)
        # except:
        #     stock_data = None
        #
        # if stock_data is None:
        #     continue

        try:
            stock_data = read(str_stock_id)

            s_stock = get_predict_evaluate(stock_data, float(df_prices.loc[str_stock_id]))
        except Exception as e:
            print("Unexpected error:", e)
            traceback.print_tb(e.__traceback__)
            print('Get error when get stock ', stock_id, ' stock_data = ', stock_data)
            error_stock_ids.append(stock_id)
            s_stock = None
        except:
            print("Unexpected error:", sys.exc_info()[0])
            error_stock_ids.append(stock_id)
            s_stock = None

        if s_stock is None:
            continue
        if df_predictions is None:
            df_predictions = pd.DataFrame(columns=s_stock.index, data=[s_stock.values])
            df_predictions['股號'] = [str_stock_id]
            df_predictions = df_predictions.set_index('股號')
            print("first record")
            print(df_predictions)
            print("index = ", df_predictions.index)
        else:
            print('get index = ', df_predictions.index)
            df_predictions.loc[str_stock_id] = s_stock.values

        print('result = ', df_predictions)
        print('err_id = ', error_stock_ids)
    output_path = gen_output_path('data', 'evaluations.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        df_predictions.to_excel(writer, sheet_name='predictions')



def generate_predictions(stock_ids=[]):
    try:
        with open(gen_output_path('data', 'evaluations.xlsx'), 'rb') as file:
            df_predictions = pd.read_excel(file, sheet_name='predictions', dtype={'股號': str})
            df_predictions = df_predictions.set_index('股號')
    except FileNotFoundError as err:
        df_predictions = None
    # print('index =', df_predictions.index)
    # print(df_predictions)
    # df_predictions.reset_index()
    # print('index =', df_predictions.index)
    # df_predictions.reindex(df_predictions.index.astype('str'))
    # print('index =', df_predictions.index)
    #
    # print(df_predictions)
    tor_proc = None
    if not _tor_process_exists():
        print('open tor process')
        tor_proc = _launch_tor()

    controller = Controller.from_port(port=ctrl_port)
    controller.authenticate()
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", proxy_port)
    socket.socket = socks.socksocket

    newIP = urlopen("http://icanhazip.com").read()
    print("NewIP Address: %s" % newIP)


    error_stock_ids = []
    for stock_id in stock_ids:
        str_stock_id = str(stock_id)
        if df_predictions is not None and str_stock_id in df_predictions.index:
            continue
        try:
            stock_data = get_stock_data(stock_id)
        except:
            stock_data = None

        if stock_data is None:
            continue
        break_loop = False
        while True:
            try:
                s_stock = get_predict_evaluate(stock_data)
                break
            except (IndexError, requests.exceptions.ConnectionError, NewConnectionError, socks.SOCKS5Error) as e:
                print("get exception: ", e, " for ", stock_id)
                controller.signal(Signal.NEWNYM)
                if not controller.is_newnym_available():
                    print("Waiting time for Tor to change IP: " + str(controller.get_newnym_wait()) + " seconds")
                    time.sleep(controller.get_newnym_wait())
                    newIP = urlopen("http://icanhazip.com").read()
                    print("NewIP Address: %s" % newIP)

            except Exception as e:
                print("Unexpected error:", e)
                traceback.print_tb(e.__traceback__)
                print('Get error when get stock ', stock_id, ' stock_data = ', stock_data)
                error_stock_ids.append(stock_id)
                # break_loop = True
                s_stock = None
                break
        if break_loop:
            break
        if s_stock is None:
            continue
        if df_predictions is None:
            # df_predictions = pd.DataFrame(columns=s_stock.index, data=[s_stock.values], index=[str_stock_id])
            df_predictions = pd.DataFrame(columns=s_stock.index, data=[s_stock.values])
            df_predictions['股號'] = [str_stock_id]
            df_predictions = df_predictions.set_index('股號')
            print("first record")
            print(df_predictions)
            print("index = ", df_predictions.index)
        else:
            print('get index = ', df_predictions.index)
            df_predictions.loc[str_stock_id] = s_stock.values
        
    print('result = ', df_predictions)
    output_path = gen_output_path('data', 'evaluations.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        df_predictions.to_excel(writer, sheet_name='predictions')
    # print('error_ids = ', error_stock_ids)

    controller.close()
    tor_proc.terminate()


def get_stock_data(stock_id, sync_to_latest=False):
    from stock_data import read
    str_stock_id = str(stock_id)
    stock_data = read(str_stock_id)
    if stock_data is None or sync_to_latest:
        try:
            print('get stock_data for ', stock_id)
            # stock_data = get_evaluate_performance(str_stock_id, 2014)
            stock_data = sync_data(str_stock_id)
            from stock_data import store
            store(stock_data)
        except (IndexError, KeyError, AttributeError) as e:
            print("get exception: ", e, " for ", stock_id)
            traceback.print_tb(e.__traceback__)
            return None

    return stock_data


def create_stock_datas(stock_codes=None):
    print('stock_codes = ', stock_codes)
    if stock_codes is None:
        stock_codes = get_stock_codes_from_twse()
    retry = 0
    for stock_code in stock_codes:
        get_data = False
        while get_data is False:
            try:
                get_stock_data(str(stock_code), True)
                get_data = True
                retry = 0
            except Exception as e:
                retry += 1
                print("get exception", e)
                traceback.print_tb(e.__traceback__)
                if retry >= 10:
                    print("retry for 10 times to get stock ", stock_code)
                    exit(-1)
                else:
                    time.sleep(60 * 10)


def get_matrix_level(stock_id, since_year, to_year=None):
    params = normalize_params(stock_id, since_year, to_year)
    if params is None:
        return None

    dfs = []
    stock_id = params.get('stock_id')
    since_year = params.get('since_year')
    to_year = params.get('to_year')
    for year in range(since_year, to_year + 1):
        matrix_level = _get_matrix_level_in_year(stock_id, year)
        if matrix_level is not None:
            dfs.append(matrix_level)
    data_frame = None if len(dfs) == 0 else pd.concat(dfs)
    # print(data_frame)
    return data_frame


MATRIX_EVALUATE_RECENT = 0
MATRIX_EVALUATE_RELATIVE = 1


def _get_matrix_level_in_year(stock_id, year, recent=False, evaluate_method=MATRIX_EVALUATE_RELATIVE):
    if year is None:
        return None
    if recent is False:
        roe = get_roe_in_year(stock_id, year)
    else:
        if evaluate_method == MATRIX_EVALUATE_RELATIVE:
            roe = get_predict_roe_by_relative(stock_id)
        else:
            roe = get_predict_roe_by_recent_four_season(stock_id)

    print('roe = ', roe)
    cash_flow_per_share_df = get_cash_flow_per_share(stock_id, since={'year': year, 'season': 1}, to={'year': year,
                                                                                                      'season': 4}) if recent is False else get_cash_flow_per_share_recent(
        stock_id)
    results_index = cash_flow_per_share_df.index
    if roe is None or cash_flow_per_share_df is None:
        print('roe = ', roe)
        print('cash_flow_per_share_df = ', cash_flow_per_share_df)
        return None
    cash_flow_per_share = cash_flow_per_share_df['每股業主盈餘現金流'].sum()
    matrix_level = _get_matrix_level(roe, cash_flow_per_share)
    print(year, ': roe = ', roe, ' 每股業主盈餘現金流 = ', cash_flow_per_share, ' matrix level = ', matrix_level)
    p_index = pd.PeriodIndex([str(year if not recent else results_index[0].qyear)], freq='A')
    return pd.DataFrame({"ROE": roe, "每股自由現金流": cash_flow_per_share, "矩陣等級": matrix_level, "近四季": recent},
                        index=p_index)


def _get_matrix_level(roe, cash_flow_per_share):
    if roe >= 0.15:
        return "A" if cash_flow_per_share > 0 else "B1"
    if roe >= 0.10:
        return "B2" if cash_flow_per_share > 0 else "C"
    if roe > 0:
        return "C1" if cash_flow_per_share > 0 else "C2"
    return "D"


def get_cash_flow_per_share_recent(stock_id):
    count = 4
    has_valid_data = False
    time_lines = None
    while not has_valid_data:
        time_lines = get_recent_seasons(count)
        if get_cash_flow_per_share(stock_id, time_lines[3]) is not None:
            break
        else:
            count += 1
    print('get_cash_flow_per_share_recent count = ', count)
    df_result = get_cash_flow_per_share(stock_id, time_lines[0])
    print('get_cash_flow_per_share_recent result = ', df_result)
    return df_result


def get_cash_flow_per_share(stock_id, since, to=None):
    cash_flow_processor = CashFlowStatementProcessor(stock_id)

    data_frame = cash_flow_processor.get_data_frames(since, to)
    if data_frame is None:
        return None
    print('get_cash_flow_per_share since ', since.get('year'), ' data_frame = ', data_frame)

    stock_count_processor = StockCountProcessor()
    try:
        stock_count = stock_count_processor.get_stock_count(stock_id, since.get('year'))
    except:
        stock_count = stock_count_processor.get_stock_count(stock_id, int(since.get('year')) - 1)
    data_frame_per_share = pd.DataFrame(
        {'每股業主盈餘現金流': pd.Series([cf / stock_count * 1000 for cf in data_frame['業主盈餘現金流']]).values}
        , index=data_frame.index)
    print(data_frame_per_share)
    return data_frame_per_share


def get_stock_codes_from_twse():
    stock_list = list(filter(lambda x: x.type == '股票', list(twstock.codes.values())))
    stock_codes = list(map(lambda x: x.code, stock_list))
    return stock_codes


def get_stock_codes(stock_type='上市', from_item=None):
    if stock_type == '上市':
        df_stocks = pd.read_csv(gen_output_path('data', '上市.csv'), engine='python', encoding='big5')
        print(df_stocks.loc[:, ['公司代號', '公司簡稱']])
        list_stocks = df_stocks.loc[:, '公司代號'].values.tolist()
    else:
        if stock_type == '上櫃':
            df_stocks = pd.read_csv(gen_output_path('data', '上櫃.csv'), engine='python', encoding='big5')
            list_stocks = df_stocks.loc[:, '公司代號'].values.tolist()
        else:
            list_stocks = None
    print(list_stocks)
    if list_stocks is not None and from_item is not None:
        list_stocks = list_stocks[list_stocks.index(from_item):]
        print(list_stocks)

    return list_stocks
