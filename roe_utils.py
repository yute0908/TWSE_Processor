import pandas as pd

from tabulate import tabulate

from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from utils import get_recent_seasons, get_time_lines


def get_in_season(stock_id, year, season):
    shareholder_equity_processor = ShareholderEquityProcessor(stock_id)
    df_shareholder_equity = shareholder_equity_processor.get_data_frame(year, season)
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    df_income_statement = income_statement_processor.get_data_frame(year, season)
    if df_shareholder_equity is None or df_income_statement is None:
        return None
    str_period = "{}Q{}".format(year, season)
    print(df_shareholder_equity.loc[str_period])
    print(df_income_statement.loc[str_period])
    roe = df_income_statement.loc[str_period, '稅後淨利'] / ((df_shareholder_equity.loc[str_period, ('權益總額', '期初餘額')] +
                                                          df_shareholder_equity.loc[str_period, ('權益總額', '期末餘額')]) / 2)
    print(roe)
    return roe


def get_recent_four_season(stock_id):
    count = 4
    roe = None

    while roe is None:
        time_lines = get_recent_seasons(count)
        roe = _get_for_times(stock_id, time_lines[0:4])
        count += 1
    return roe


def get_in_year(stock_id, year):
    time_lines = get_time_lines(since={'year': year, 'season': 1}, to={'year': year, 'season': 4})
    print(time_lines)
    print("get in year ", year, ":", _get_for_times(stock_id, time_lines=time_lines))


def _get_for_times(stock_id, time_lines):
    list_df_income_statement = _get_income_statements(stock_id, time_lines)
    length = len(time_lines)
    if list_df_income_statement is None:
        return None
    start_period = "{}Q{}".format(time_lines[0].get('year'),
                                  time_lines[0].get('season'))
    stop_period = "{}Q{}".format(time_lines[length - 1].get('year'),
                                 time_lines[length - 1].get('season'))
    df_income_statement = pd.concat(list_df_income_statement)

    print("稅後淨利總和", df_income_statement['稅後淨利'].sum())
    shareholder_equity_processor = ShareholderEquityProcessor(stock_id)
    df_shareholder_equity = shareholder_equity_processor.get_data_frames(since=time_lines[0],
                                                                         to=time_lines[length - 1])
    print("\n")
    print(df_shareholder_equity)
    roe = df_income_statement['稅後淨利'].sum() / ((df_shareholder_equity.loc[start_period, ('權益總額', '期初餘額')] +
                                                df_shareholder_equity.loc[stop_period, ('權益總額', '期末餘額')]) / 2)
    print("roe = ", roe)
    return roe


def _get_income_statements(stock_id, time_lines):
    reversed_time_lines = time_lines[::-1]
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    list_df_income_statement = []
    for i in range(len(reversed_time_lines)):
        df_income_statement = income_statement_processor.get_data_frame(reversed_time_lines[i].get('year'),
                                                                        reversed_time_lines[i].get('season'))
        if df_income_statement is None:
            return None
        else:
            list_df_income_statement.append(df_income_statement)
    return list_df_income_statement[::-1]