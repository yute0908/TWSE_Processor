from datetime import datetime

from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
import os
import traceback

from tabulate import tabulate


class Crawler:

    def get_type_code(self, page):
        typeks = self.get_options(page, 'value', 'select[name=\"TYPEK\"] > option')
        codes = self.get_options(page, 'value', 'select[name=\"code\"] > option')
        return typeks, codes

    def get_options(self, page, field, condition):
        res = page.content
        soup = BeautifulSoup(res, 'html.parser')
        return [[x[field], x.string] for x in soup.select(condition) if x.string]

    def get_table(self, session, url, payload, **kwargs):
        # use payload
        response = session.post(url, data=payload)
        res = response.content
        soup = BeautifulSoup(res, 'html.parser')
        table = soup.find("table", kwargs)
        return table


def get_dataframe(table):
    raws = table.find_all('tr')
    # get the header
    header = raws[0].find_all('th')
    header_of_table = [x.get_text() for x in header]
    # get the cell
    list_of_table = []
    for raw in raws:
        r = [x.get_text() for x in raw.find_all('td')]
        if len(r) > 0:
            list_of_table.append(r)

    df = pd.DataFrame(list_of_table, columns=header_of_table)
    return df


def gen_output_path(directory, filename=None):
    if directory:
        os.makedirs(directory, exist_ok=True)
        if not filename:
            return os.path.join(directory)
        else:
            return os.path.join(directory, filename)
    else:
        return filename


import traceback


def get_list_of_company():
    # get the session
    url = 'http://mops.twse.com.tw/mops/web/t51sb01'
    session = requests.Session()
    result = session.get(url)
    if result.ok is False:
        return

    crawler = Crawler()
    typeks = crawler.get_options(result, 'value', 'select[name=\"TYPEK\"] > option')
    codes = crawler.get_options(result, 'value', 'select[name=\"code\"] > option')
    url2 = 'http://mops.twse.com.tw/mops/web/ajax_t51sb01'
    count = 0
    for typek in typeks:
        for code in codes:
            try:
                table = crawler.get_table(session, url2, {'encodeURIComponent': '1',
                                                          'step': '1',
                                                          'firstin': '1',
                                                          'TYPEK': typek[0],
                                                          'code': code[0]}, style='width:100%;')
                df = get_dataframe(table)
                out_excel_name = ('twse_%s_%s.xlsx' % (typek[1], code[1]))
                print(df)
                df.to_excel(gen_output_path('data', out_excel_name), index=False, encoding='UTF-8')

            except:
                print('%s, %s faild' % (typek[1], code[1]))
                traceback.print_exc()
            time.sleep(10)


