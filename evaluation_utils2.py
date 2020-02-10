import time
import traceback
from datetime import datetime

import pandas as pd

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.dividend_policy import DividendPolicyProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from rdss.shareholder_equity import ShareholderEquityProcessor
from rdss.stock_count import StockCountProcessor
from stock_data import store_df
from value_measurement import PriceMeasurementProcessor, IndexType


def sync_statements(stock_codes):
    print('stock_codes = ', stock_codes)
    retry = 0
    for stock_code in stock_codes:
        get_data = False
        while get_data is False:
            try:
                _sync_statements(stock_code)
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


def _sync_performance(df_statements):
    df_profit_statement = df_statements['profit_statement']
    df_balance_sheet = df_statements['balance_sheet']
    df_cash_flow_statement = df_statements['balance_sheet']
    df_dividend_policy = df_statements['dividend_policy']
    print(df_profit_statement)
    print(df_balance_sheet)
    print(df_cash_flow_statement)
    print(df_dividend_policy)


def _sync_statements(stock_id):
    start_year = 2013
    df_profit_statement = _sync_profit_statement(start_year, stock_id)
    df_balance_sheet = _sync_balance_sheet(start_year, stock_id)
    df_cash_flow_statement = _sync_cash_flow_statement(start_year, stock_id)
    df_dividend_policy = _sync_dividend_policy(start_year, stock_id)
    print('df_profit_statement = ', df_profit_statement)
    print('df_balance_sheet = ', df_balance_sheet)
    print('df_cash_flow_statement = ', df_cash_flow_statement)
    print('df_dividend_policy = ', df_dividend_policy)
    if df_profit_statement is None or df_balance_sheet is None or df_cash_flow_statement is None or df_dividend_policy\
            is None:
        return None
    store_df(stock_id, {'profit_statement': df_profit_statement,
                        'balance_sheet': df_balance_sheet,
                        'cash_flow_statement':  df_cash_flow_statement,
                        'dividend_policy': df_dividend_policy},
             filename='statments_{0}.xlsx'.format(stock_id))


def _sync_cash_flow_statement(start_year, stock_id):
    cash_flow_processor = CashFlowStatementProcessor(stock_id)
    df_cash_flow_statement = cash_flow_processor.get_data_frames({'year': start_year - 1})
    print('df_cash_flow_statement = ', df_cash_flow_statement)
    return df_cash_flow_statement


def _sync_balance_sheet(start_year, stock_id):
    balance_sheet_processor = SimpleBalanceSheetProcessor(stock_id)
    df_balance_statement = balance_sheet_processor.get_data_frames({'year': start_year - 1})
    shareholder_equity_processor = ShareholderEquityProcessor(stock_id)
    df_shareholder_equity = shareholder_equity_processor.get_data_frames({'year': start_year - 1})
    print('df_balance_statement = ', df_balance_statement)
    print('df_shareholder_equity = ', df_shareholder_equity)
    if df_balance_statement is None or df_shareholder_equity is None:
        return None
    df_combine = df_balance_statement.join(df_shareholder_equity, how='outer')
    indexes = df_combine[df_combine['每股淨值'].isna()].index
    df_combine.drop(indexes, inplace=True)
    print('合併 = ', df_combine)
    return df_combine


def _sync_profit_statement(start_year, stock_id):
    income_statement_processor = SimpleIncomeStatementProcessor(stock_id)
    df_income_statement = income_statement_processor.get_data_frames({'year': start_year - 1})
    print('df_income_statement = ', df_income_statement)
    return df_income_statement


def _sync_dividend_policy(start_year, stock_id):
    # now = datetime.now()
    dividend_policy_processor = DividendPolicyProcessor(stock_id)
    # df_dividend_policy = dividend_policy_processor.get_data_frames({'year': start_year - 1}, {'year': now.year})
    df_dividend_policy = dividend_policy_processor.get_data_frames({'year': start_year - 1})

    stock_count_processor = StockCountProcessor()
    df_stock_count = stock_count_processor.get_data_frame(stock_id, start_year)

    price_measurement_processor = PriceMeasurementProcessor(stock_id)
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
