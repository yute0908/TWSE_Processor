import csv
import logging
import re
import time
from os import mkdir
from os.path import isdir

import pandas as pd
import requests


class Crawler():
    def __init__(self, prefix="data"):
        ''' Make directory if not exist when initialize '''
        if not isdir(prefix):
            mkdir(prefix)
        self.prefix = prefix

    def _clean_row(self, row):
        ''' Clean comma and spaces '''
        for index, content in enumerate(row):
            row[index] = re.sub(",", "", content.strip())
        return row

    def _record(self, stock_id, row):
        ''' Save row to csv file '''
        f = open('{}/{}.csv'.format(self.prefix, stock_id), 'a')
        cw = csv.writer(f, lineterminator='\n')
        cw.writerow(row)
        f.close()

    def _get_tse_data(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }

        print('query_params = ', query_params)
        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            return None

        content = page.json()
        # print(content)

        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])

        format_data = {}
        for data in content['data9']:
            sign = '-' if data[9].find('green') > 0 else ''
            row = self._clean_row([
                # date_str_mingguo, # 日期
                data[2], # 成交股數
                data[4], # 成交金額
                data[5], # 開盤價
                data[6], # 最高價
                data[7], # 最低價
                data[8], # 收盤價
                sign + data[10], # 漲跌價差
                data[3], # 成交筆數
            ])
            format_data[data[0]] = row
        return format_data
            # self._record(data[0].strip(), row)

    def _get_otc_data(self, date_tuple):
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        print('url = ', url)
        page = requests.get(url,"\n")

        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
            return None

        result = page.json()

        if result['reportDate'] != date_str:
            logging.error("Get error date OTC data at {}".format(date_str))
            return None

        # print('get ', [result['mmData'], result['aaData']])

        i = 0
        # '''
        format_data = {}
        for table in [result['mmData'], result['aaData']]:
            i += 1
            # print('i = ', i)
            for tr in table:
                row = self._clean_row([
                    # date_str,
                    tr[8], # 成交股數
                    tr[9], # 成交金額
                    tr[4], # 開盤價
                    tr[5], # 最高價
                    tr[6], # 最低價
                    tr[2], # 收盤價
                    tr[3], # 漲跌價差
                    tr[10] # 成交筆數
                ])
                format_data[tr[0]] = row
                # self._record(tr[0], row)
        # '''
        return format_data


    def get_data(self, date_tuple):
        print('Crawling {}\n'.format(date_tuple))
        tse_data = self._get_tse_data(date_tuple)
        otc_data = self._get_otc_data(date_tuple)
        tse_data.update(otc_data)
        df = pd.DataFrame(tse_data, index=['成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數'])
        return df.T
