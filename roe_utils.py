from tabulate import tabulate

from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from utils import get_recent_seasons


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
    time_lines = get_recent_seasons(count)
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    while count > 0:
        count = count - 1
        df_income_statement = income_statement_processor.get_data_frame(time_lines[count].get('year'),
                                                                        time_lines[count].get('season'))
        print('income data frame', df_income_statement)
        if df_income_statement is not None:
            break
