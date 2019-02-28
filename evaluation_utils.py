import pandas as pd
from datetime import datetime

from tabulate import tabulate

from rdss.cashflow_statment import CashFlowStatementProcessor
from rdss.stock_count import StockCountProcessor
from roe_utils import get_roe_in_year


def get_matrix_level(stock_id, since_year, to_year=None):
    if since_year is None:
        return None

    get_current = False
    if to_year is not None and since_year > to_year:
        return None
    else:
        if to_year is None:
            get_current = True
            to_year = datetime.now().year

    dfs = []
    for year in range(since_year, to_year + 1):
        matrix_level = _get_matrix_level_in_year(stock_id, year)
        if matrix_level is not None:
            dfs.append(matrix_level)
    data_frame = None if len(dfs) == 0 else pd.concat(dfs)
    print(data_frame)
    return data_frame

def _get_matrix_level_in_year(stock_id, year):
    roe = get_roe_in_year(stock_id, year)
    cash_flow_per_share_df = get_cash_flow_per_share(stock_id, {'year': year})
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
