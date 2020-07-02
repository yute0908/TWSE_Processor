from datetime import datetime

import pandas as pd

from tabulate import tabulate

from rdss.statement_fetchers import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from utils import get_recent_seasons, get_time_lines


def get_roe_in_season(stock_id, year, season):
    roe = _get_for_times(stock_id, [{'year': year, 'season': season}])
    print('roe = ', roe)
    return roe


def get_predict_roe_by_recent_four_season(stock_id):
    count = 4
    roe = None

    while roe is None:
        time_lines = get_recent_seasons(count)
        roe = _get_for_times(stock_id, time_lines[0:4])
        count += 1
    return roe

def get_predict_roe_by_relative(stock_id):
    now_year = datetime.now().year
    time_lines = get_time_lines(since={'year': now_year, 'season': 1})

    # list_temp_times = [time for time in time_lines[::-1] if _get_for_times(stock_id, [time]) is not None]
    # print(list_temp_times)
    last_time_available = next((time for time in time_lines[::-1] if _get_for_times(stock_id, [time]) is not None), None)
    print('last_time_available = ', last_time_available)
    if last_time_available is None:
        return None

    roe_current = _get_for_times(stock_id, time_lines[0: time_lines.index(last_time_available) + 1])
    roe_last_year_relative = _get_for_times(stock_id, get_time_lines(since={'year': now_year - 1, 'season': 1},
                                            to={'year': now_year - 1, 'season': last_time_available.get('season')}))
    roe_last_year = get_roe_in_year(stock_id, now_year - 1)
    print('roe_current = ', roe_current, ' roe_last_year_relative = ', roe_last_year_relative, ' roe_last_year = ', roe_last_year)

    if roe_current is None or roe_last_year_relative is None or roe_last_year is None:
        return None

    roe_relative = roe_last_year * (roe_current / roe_last_year_relative)

    print('roe_relative = ', roe_relative)
    return roe_relative

def get_roe_in_year(stock_id, year):
    time_lines = get_time_lines(since={'year': year, 'season': 1}, to={'year': year, 'season': 4})
    roe = _get_for_times(stock_id, time_lines=time_lines)
    print("get in year ", year, ":", roe)
    return roe


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
    print('df_shareholder_equity = ', df_shareholder_equity)
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