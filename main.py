from evaluation_utils import get_stock_codes
from rdss.fetch_data_utils import get_simple_balance_sheet_raw_data, get_simple_balance_sheet_raw_datas

if __name__ == "__main__":
    stock_code_list = get_stock_codes(stock_type='上市')
    stock_code_list.extend(get_stock_codes(stock_type='上櫃'))
    get_simple_balance_sheet_raw_datas(stock_code_list[0: 10])
