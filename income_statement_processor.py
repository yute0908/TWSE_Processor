import traceback

import pandas as pd
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from twse_crawler import gen_output_path


class IncomeStatementProcessor:
    def get_income_statement(self, stock_id, since, to=None):
        params_lists = self.get_requests_list(stock_id, since=since, to=to)
        url = 'http://mops.twse.com.tw/mops/web/ajax_t164sb04'
        session = requests.Session()

        dfs = []
        for params in params_lists:
            result = session.post(url, params)
            if result.ok is False:
                print('get content fail')
                return
            try:
                soup = BeautifulSoup(result.content, 'html.parser')
                table = soup.find('table', attrs={"class": "hasBorder", "align": "center"})
                rows = table.find_all('tr')

            except:
                print('parse table fail')
                break

            date = datetime(params['year'] + 1911, params['season'] * 3, 1)
            str_date = datetime.strftime(date, '%Y-%m')
            df = self.parse_table(rows, str_date)
            if df is not None:
                dfs.append(df)
        # return
        return pd.concat(dfs, axis=1, sort=False)

    def parse_table(self, rows, str_date):
        print('parse_table:', str_date)
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

        return pd.DataFrame(processed_rows, columns=pd.MultiIndex.from_tuples(column_indexes, names=['時間', '金額/百分比']),
                            index=pd.MultiIndex.from_tuples(row_indexes, names=['主要項目', '次要項目']))

    def get_requests_list(self, stock_id, since=None, to=None):
        if since is None and to is None:
            print("since and to should not both be None")
            return

        now = datetime.now()
        if since is not None:
            since_year = since.get('year')
            if since_year is None:
                print("since year should not be None")
                return
            since_season = since.get('season', 1)
            if to is not None:
                to_year = to.get('year', now.year - 1911)
                to_season = to.get('season', (now.month - 1) / 3 if (to_year + 1911) == now.year else 4)
            else:
                to_year = now.year - 1911
                to_season = (now.month - 1) / 3 + 1

        else:
            since_year = to_year = to.get('year')
            since_season = to.get('season')
            if since_season is None:
                since_season = 1
                to_season = to.get('season', (now.month - 1) / 3 + 1)
            else:
                to_season = since_season

        year_count = to_year - since_year - 1
        season_count = 4 - since_season + to_season + year_count * 4 + 1
        requests_list = []
        print("get seasons")
        for i in range(int(season_count)):
            mod_season = (since_season + i) % 4
            year = since_year + int((since_season + i) / 4) - (1 if mod_season == 0 else 0)
            season = mod_season if mod_season > 0 else 4
            print("(", year, ",", season, ")")
            requests_list.append({
                'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'queryName': 'co_id', 'TYPEK': 'all',
                'isnew': 'false',
                'co_id': stock_id, 'year': year, 'season': season})
        print("\n")
        return requests_list


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


def store_data_frame(df, stock_id):
    out_excel_name = 'income_statement_{0}.xlsx'.format(stock_id)
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


if __name__ == "__main__":
    # execute only if run as a script
    income_statement_processor = IncomeStatementProcessor()
    df = income_statement_processor.get_income_statement(2330, {"year": 106})
    store_data_frame(df, 2330)
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
