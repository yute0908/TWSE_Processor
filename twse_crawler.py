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

def gen_output_path(dirtory='', filename=''):
    if not filename:
        return None

    if dirtory:
        os.makedirs(dirtory, exist_ok=True)
        return os.path.join(dirtory, filename)
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
                out_excel_name= ('twse_%s_%s.xlsx' % (typek[1], code[1]))
                print(df)
                df.to_excel(gen_output_path('data', out_excel_name), index=False, encoding='UTF-8')

            except:
                print('%s, %s faild' % (typek[1], code[1]))
                traceback.print_exc()
            time.sleep(10)


def get_income_statement(stock_id, year, season):
    url = 'http://mops.twse.com.tw/mops/web/ajax_t164sb04'
    session = requests.Session()

    result = session.post(url, {
        'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'queryName': 'co_id', 'TYPEK': 'all', 'isnew': 'false',
        'co_id': stock_id, 'year': year, 'season': season
    })

    if result.ok is False:
        print('get content fail')
        return
    soup = BeautifulSoup(result.content, 'html.parser')
    table = soup.find('table', attrs={"class": "hasBorder", "align": "center"})

    date = datetime(year + 1911, season * 3, 1)
    str_date = datetime.strftime(date, '%Y-%m')
    rows = table.find_all('tr')
    df = parse_table(rows, str_date)

    out_excel_name = 'income_statement_{0}.xlsx'.format(stock_id)
    path = gen_output_path('data', out_excel_name)
    stored_df = read_stored_data_frame(path)
    if stored_df is not None:
        print(tabulate([list(row) for row in stored_df.values], headers=list(stored_df.columns)))
        print(tabulate([list(row) for row in df.values], headers=list(df.columns)))
        result = pd.concat([stored_df, df], axis=1, sort=False)
        print(tabulate([list(row) for row in result.values], headers=list(result.columns)))
        result.to_excel(path, index=True, encoding='UTF-8')
    else:
        df.to_excel(path, index=True, encoding='UTF-8')


def parse_table(rows, str_date):
    rows_in_data_frame = []
    column_indexes = [(str_date, '金額(千元)'), (str_date, '%')]
    row_indexes = []
    for row in rows:
        r = [x.get_text() for x in row.find_all('td')]
        if len(r) > 2:
            rows_in_data_frame.append(r[0: 3])
    processed_rows = []
    main_row_index = None
    for row in rows_in_data_frame:
        row_data = ['' if not row[1].strip() else float(row[1].replace(',', '')),
                    '' if not row[2].strip() else float(row[2])]
        main_row_index = row[0] if (len(row[0]) - len(row[0].lstrip())) == 0 else main_row_index
        second_row_index = row[0]
        if not (row_data[0] == '' and row_data[1] == ''):
            processed_rows.append(row_data)
            row_indexes.append((main_row_index, second_row_index))

    data_frame = pd.DataFrame(processed_rows, columns=pd.MultiIndex.from_tuples(column_indexes, names=['時間', '金額/百分比']),
                              index=pd.MultiIndex.from_tuples(row_indexes, names=['主要項目', '次要項目']))
    return data_frame


def read_stored_data_frame(path):

    data_frame = None
    try:
        data_frame = pd.read_excel(path, index_col=[0, 1], header=[0, 1])
        # print(tabulate([list(row) for row in data_frame.values], headers=list(data_frame.columns), showindex="always"))

    except Exception as inst:
        print("get exception", inst)
        traceback.print_tb(inst.__traceback__)
    # data_frame = pd.read_excel(path)

    return data_frame


if __name__ == "__main__":
    # execute only if run as a script
    get_income_statement(2330, 106, 4)
    # print(integer)
    # read_dataframe('data/income_statement_{0}.xlsx'.format(2330))
