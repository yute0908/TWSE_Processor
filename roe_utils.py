from tabulate import tabulate

from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor


def get_in_season(stock_id, year, season):
    shareholder_equity_processor = ShareholderEquityProcessor(stock_id)
    df_shareholder_equity = shareholder_equity_processor.get_data_frame(year, season)
    str_period = "{}Q{}".format(year, season)
    print(df_shareholder_equity.loc[str_period])
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    df_income_statement = income_statement_processor.get_data_frame(year, season)
    print(df_income_statement.loc[str_period])
    roe = df_income_statement.loc[str_period, '稅後淨利'] / ((df_shareholder_equity.loc[str_period, ('權益總額', '期初餘額')] +
                                                          df_shareholder_equity.loc[str_period, ('權益總額', '期末餘額')])/2)
    print(roe)