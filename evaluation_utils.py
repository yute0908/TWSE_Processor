import math
import socket
import sys
import time
import traceback
from urllib.request import urlopen

import pandas as pd
import requests
import socks
import twstock
from selenium.common.exceptions import WebDriverException
from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config
from twstock import Stock
from urllib3.exceptions import NewConnectionError

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.stock_count import StockCountProcessor
from rdss.utils import normalize_params
from roe_utils import get_roe_in_year
from stock_data import StockData
from twse_crawler import gen_output_path
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
    print(data_frame)
    return data_frame


def _get_matrix_level_in_year(stock_id, year):
    roe = get_roe_in_year(stock_id, year)
    cash_flow_per_share_df = get_cash_flow_per_share(stock_id, since={'year': year, 'season': 1},
                                                     to={'year': year, 'season': 4})
    if roe is None or cash_flow_per_share_df is None:
        return None
    cash_flow_per_share = cash_flow_per_share_df['每股業主盈餘現金流'].sum()
    matrix_level = _get_matrix_level(roe, cash_flow_per_share)
    print(year, ': roe = ', roe, ' 每股業主盈餘現金流 = ', cash_flow_per_share, ' matrix level = ', matrix_level)
    p_index = pd.PeriodIndex([str(year)], freq='A')
    return pd.DataFrame({"roe": roe, "每股自由現金流": cash_flow_per_share, "矩陣等級": matrix_level},
                        index=p_index)


def _get_matrix_level(roe, cash_flow_per_share):
    if roe >= 0.15:
        return "A" if cash_flow_per_share > 0 else "B1"
    if roe >= 0.10:
        return "B2" if cash_flow_per_share > 0 else "C"
    if roe > 0:
        return "C1" if cash_flow_per_share > 0 else "C2"
    return "D"


def get_cash_flow_per_share(stock_id, since, to=None):
    cash_flow_processor = CashFlowStatementProcessor(stock_id)
    data_frame = cash_flow_processor.get_data_frames(since, to)

    if data_frame is None:
        return None
    stock_count_processor = StockCountProcessor()
    stock_count = stock_count_processor.get_stock_count(stock_id, since.get('year'))
    data_frame_per_share = pd.DataFrame(
        {'每股業主盈餘現金流': pd.Series([cf / stock_count * 1000 for cf in data_frame['業主盈餘現金流']]).values}
        , index=data_frame.index)
    print(data_frame_per_share)
    return data_frame_per_share


def get_evaluate_performance(stock_id, since_year, to_year=None):
    params = normalize_params(stock_id, since_year, to_year)
    print(params)
    if params is None:
        return None

    stock_id = params.get('stock_id')
    since_year = params.get('since_year')
    to_year = params.get('to_year')
    get_recent_four_seasons = params.get('get_resent_four_seasons')

    dividend_policy_processor = DividendPolicyProcessor(stock_id)
    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': to_year}, {'year': to_year + 1})

    try:
        should_modify_since_year = False if df_dividend_policy is not None and str(
            since_year) in df_dividend_policy.index else True
    except KeyError:
        should_modify_since_year = True
        print('get error')
    since_year = since_year - 1 if should_modify_since_year and since_year >= to_year - 1 else since_year

    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': since_year}, {'year': to_year + 1})
    df_dividend_policy.sort_index(inplace=True)
    print('df_dividend_policy = ', df_dividend_policy)

    df_income_statement = SimpleIncomeStatementProcessor(stock_id).get_data_frames({'year': since_year - 1},
                                                                                   {'year': to_year})
    df_income_statement.sort_index(inplace=True)
    print('df_income_statement = ', df_income_statement)

    balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
    df_balance_sheet = balance_sheet_processor.get_data_frames({'year': since_year - 1},
                                                               {'year': to_year})
    print('df_balance_sheet = ', df_balance_sheet)

    price_measurement_processor = PriceMeasurementProcessor(stock_id)
    df_prices = price_measurement_processor.get_data_frame()

    results_data = []
    results_index = []
    for year in range(since_year, to_year + 1):
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
    return StockData(stock_id, pd.DataFrame(results_data, index=results_index), merged_statement)


def get_predict_evaluate(stock_data):
    stock = Stock(stock_data.stock_id)
    # print(stock.price)
    eps_last_four_season = stock_data.df_statement.iloc[-4:].loc[:, 'EPS'].sum()
    latest_price = next((value for value in reversed(stock.price) if value is not None), 0.0)
    print('stock_price = ', latest_price, ' eps_last_four_season = ', eps_last_four_season)
    predict_pe = latest_price / eps_last_four_season
    g = stock_data.df_performance.iloc[-1].at['保留盈餘成長率']
    y = 0.0 if latest_price <= 0 else stock_data.df_performance.iloc[-1].at['現金股利'] / latest_price
    print('y = ', y, 'g = ', g)
    peter_lynch_value = (y + g) / predict_pe * 100 if predict_pe > 0 else 0
    print('本益比 = ', predict_pe, '彼得林區評價 = ', peter_lynch_value)

    def peter_lynch_reverse(val):
        return (g + math.sqrt(math.pow(g, 2) + 4 * stock_data.df_performance.iloc[-1].at['現金股利'] * (
                val / 100) / eps_last_four_season)) / (
                       2 * (val / 100) / eps_last_four_season)

    print('彼得林區評價 2 = ', peter_lynch_reverse(2.0), ' 彼得林區評價 1.5 = ', peter_lynch_reverse(1.5), '彼得林區評價 1 = ',
          peter_lynch_reverse(1.0))
    return pd.Series(
        {'股價': latest_price, '本益比': predict_pe, '彼得林區評價': peter_lynch_value, '彼得林區評價2倍股價': peter_lynch_reverse(2.0),
         '彼得林區評價1.5倍股價': peter_lynch_reverse(1.5), '彼得林區評價1倍股價': peter_lynch_reverse(1.0)})


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
    controller.close()
    tor_proc.terminate()

    print('result = ', df_predictions)
    output_path = gen_output_path('data', 'evaluations.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        df_predictions.to_excel(writer, sheet_name='predictions')
    print('error_ids = ', error_stock_ids)


def get_stock_data(stock_id):
    from stock_data import read
    str_stock_id = str(stock_id)
    stock_data = read(str_stock_id)
    if stock_data is None:
        try:
            print('get stock_data for ', stock_id)
            stock_data = get_evaluate_performance(str_stock_id, 2014)
            from stock_data import store
            store(stock_data)
        except IndexError as inst:
            print("get exception IndexError: ", inst, " for ", stock_id)
            traceback.print_tb(inst.__traceback__)
            return None
        except KeyError as ke:
            print("get exception KeyError: ", ke, " for ", stock_id)
            traceback.print_tb(ke.__traceback__)
            return None
        except AttributeError as ae:
            print("get exception AttributeError: ", ae, " for ", stock_id)
            traceback.print_tb(ae.__traceback__)
            return None

    return stock_data


def create_stock_datas(stock_codes=None):
    print('stock_codes = ', stock_codes)
    if stock_codes is None:
        stock_list = list(filter(lambda x: x.type == '股票', list(twstock.codes.values())))
        stock_codes = list(map(lambda x: x.code, stock_list))
    retry = 0
    for stock_code in stock_codes:
        get_data = False
        while get_data is False:
            try:
                get_stock_data(str(stock_code))
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


def get_stock_codes(stock_type='上市'):
    if stock_type == '上市':
        df_stocks = pd.read_csv(gen_output_path('data', '上市.csv'), engine='python')
        list_stocks = df_stocks.loc[:, '公司代號'].values.tolist()
    else:
        if stock_type == '上櫃':
            df_stocks = pd.read_csv(gen_output_path('data', '上櫃.csv'), engine='python')
            list_stocks = df_stocks.loc[:, '公司代號'].values.tolist()
        else:
            list_stocks = None
    print(list_stocks)
    return list_stocks
