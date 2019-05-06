import socket
import time
import traceback
from urllib.request import urlopen

import pandas as pd
import socks
from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config
from torrequest import TorRequest

from rdss.balance_sheet import SimpleBalanceSheetProcessor
from rdss.income_statement import SimpleIncomeStatementProcessor
from twse_crawler import gen_output_path


def read_data_frame(path):
    data_frame = None
    try:
        data_frame = pd.read_excel(path, index_col=[0, 1], header=[0, 1])
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    except Exception as inst:
        print("get exception", inst)
        traceback.print_tb(inst.__traceback__)
    # data_frame = pd.read_excel(path)

    return data_frame


def store_data_frame(df, out_excel_name):
    # out_excel_name = 'income_statement_{0}.xlsx'.format(stock_id)
    path = gen_output_path('data', out_excel_name)
    df.to_excel(path, index=True, encoding='UTF-8')


# def store_data_frame(df, stock_id):
#     out_excel_name = 'income_statement_{0}.xlsx'.format(stock_id)
#     path = gen_output_path('data', out_excel_name)
#     stored_df = read_data_frame(path)
#     if stored_df is not None:
#         # print(tabulate([list(row) for row in df.values], headers=list(df.columns)))
#         result = pd.concat([stored_df, df], axis=1, sort=False)
#         # print(tabulate([list(row) for row in result.values], headers=list(result.columns)))
#         result.to_excel(path, index=True, encoding='UTF-8')
#     else:
#         df.to_excel(path, index=True, encoding='UTF-8')


def _tor_process_exists():
    try:
      ctrl = Controller.from_port(port=9051)
      ctrl.close()
      return True
    except:
      return False

def _launch_tor():
    return launch_tor_with_config(
      config={
        'SocksPort': str(9050),
        'ControlPort': str(9051)
      },
      take_ownership=True)

if __name__ == "__main__":
    # execute only if run as a script
    '''
    stock_id = 2330
    start = {"year": 2016, "season": 1}
    processor = SimpleIncomeStatementProcessor(stock_id)
    df = processor.get_data_frames(start)
    store_data_frame(df, 'income_statement_{0}.xlsx'.format(stock_id))

    processor = SimpleBalanceSheetProcessor(stock_id)
    df = processor.get_data_frames(start)
    store_data_frame(df, 'balance_sheet_{0}.xlsx'.format(stock_id))
    '''
    '''
    with TorRequest() as tr:
        response = tr.get('http://ipecho.net/plain')
        print(response.text)
    '''
    tor_proc = None
    if not _tor_process_exists():
        print('open tor process')
        tor_proc = _launch_tor()

    with Controller.from_port(port=9051) as controller:
        # controller.authenticate(password='my_pwd')
        controller.authenticate()
        socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050)
        socket.socket = socks.socksocket

        for i in range(0, 3):
            newIP = urlopen("http://icanhazip.com").read()
            print("NewIP Address: %s" % newIP)
            controller.signal(Signal.NEWNYM)
            if not controller.is_newnym_available():
                print("Waitting time for Tor to change IP: " + str(controller.get_newnym_wait()) + " seconds")
                time.sleep(controller.get_newnym_wait())
        controller.close()
    tor_proc.terminate()

    # store_data_frame(df, 2330)
    # data_frame = get_income_statement(2330, {"year": 107})
    # if data_frame is not None:
    #     store_data_frame(data_frame, 2330)

# def test_get_requests_list():
# get_requests_list(2330, since={"year": 107})
# get_requests_list(2330, to={"year": 107})
# get_requests_list(2330, to={"year": 107, "season": 1})
# get_requests_list(2330, to={"year": 107, "season": 2})
# get_requests_list(2330, to={"year": 107, "season": 3})
# get_requests_list(2330, to={"year": 107, "season": 4})
#
#
# get_requests_list(2330, {"year": 106})
# get_requests_list(2330, {"year": 105})
#
# get_requests_list(2330, {"year": 104}, {"year": 105})
# get_requests_list(2330, {"year": 104, "season": 2}, {"year": 105})

# get_requests_list(2330, {"year": 104, "season": 3}, {"year": 105, "season": 1})
# get_requests_list(2330, {"year": 104, "season": 2}, {"year": 105, "season": 2})
# get_requests_list(2330, {"year": 104, "season": 1}, {"year": 105, "season": 4})
# get_requests_list(2330, {"year": 104, "season": 1})
# get_requests_list(2330, {"year": 104, "season": 3})
# get_requests_list(2330, {"year": 104, "season": 4})

# get_requests_list(2330, {"year": 106}, {"year": 107})
