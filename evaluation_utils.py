import math

import pandas as pd
from twstock import Stock

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.stock_count import StockCountProcessor
from rdss.utils import normalize_params
from roe_utils import get_roe_in_year
from stock_data import StockData
from value_measurement import PriceMeasurementProcessor


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
        dividend_yield = dr_in_two_years / 2 / float(df_prices.loc[str(year), '平均股價'])
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
    print(stock.price)
    eps_last_four_season = stock_data.df_statement.iloc[-4:].loc[:, 'EPS'].sum()
    predict_pe = stock.price[-1] / eps_last_four_season
    peter_lynch_value = stock_data.df_performance.iloc[-1].at['高登模型預期報酬率'] / predict_pe * 100
    print('本益比 = ', predict_pe, '彼得林區評價 = ', peter_lynch_value)
    g = stock_data.df_performance.iloc[-1].at['保留盈餘成長率']

    def peter_lynch_reverse(val):
        return (g + math.sqrt(math.pow(g, 2) + 4 * stock_data.df_performance.iloc[-1].at['現金股利'] * (
                val / 100) / eps_last_four_season)) / (
                2 * (val / 100) / eps_last_four_season)
    print('彼得林區評價 2 = ', peter_lynch_reverse(2.0), ' 彼得林區評價 1.5 = ', peter_lynch_reverse(1.5), '彼得林區評價 1 = ',
          peter_lynch_reverse(1.0))
    return pd.Series(
        {'股價': stock.price[-1], '本益比': predict_pe, '彼得林區評價': peter_lynch_value, '彼得林區評價2倍股價': peter_lynch_reverse(2.0),
         '彼得林區評價1.5倍股價': peter_lynch_reverse(1.5), '彼得林區評價1倍股價': peter_lynch_reverse(1.0)})
