from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from tabulate import tabulate

from rdss.fetcher import DataFetcher
from rdss.statement_processor import StatementProcessor


class DividendPolicyProcessor2(StatementProcessor):

    def __init__(self):
        super().__init__(None)
        self.dividend_policy_fetcher = _DividendPolicyFetcher2()

    def get_data_frame(self, year, season):
        pass

    def get_data_frames(self, stock_id, start_year=datetime.now().year, to_year=datetime.now().year):
        return self._parse_raw_data(stock_id=stock_id, raw_data=self._get_raw_data(stock_id, start_year, to_year))

    def _parse_raw_data(self, stock_id, raw_data):
        soup = BeautifulSoup(raw_data, 'html.parser')
        # print(soup.prettify())
        table = soup.find('table', attrs={"class": "hasBorder", "width": "99%"})
        # print(table)
        data_frame = pd.read_html(str(table))[0]
        # data_frame.drop(data_frame.columns[[0, 18]], axis=1, inplace=True)
        print(tabulate(data_frame))
        data_frame = data_frame.iloc[3:, :]

        def parse_period(period_string):
            if period_string.find("年年度") > -1:
                return str(int(period_string.replace("年年度", "")) + 1911) + "Q4"
            else:
                period_strings = period_string.replace("季", "").split("年第")
                return str((int(period_strings[0]) + 1911)) + "Q" + period_strings[1]

        period_list = list(map(lambda x: pd.Period(parse_period(x)), data_frame.iloc[:, 1].tolist()))
        dividend_cash_list = list(map(lambda x: float(x), data_frame.iloc[:, 10].tolist()))
        dividend_cash_stock_list = list(map(lambda x: float(x), data_frame.iloc[:, 13].tolist()))
        dic_dividend = {'現金股利': dividend_cash_list, '配股': dividend_cash_stock_list}

        now = datetime.now()

        def get_default_time_line_periods():
            periods = []
            for year in range(2013, now.year + 1):
                for quarter in range(1, 5):
                    periods.append(pd.Period(str(year) + 'Q' + str(quarter)))
            return periods

        df_dividend = pd.DataFrame(dic_dividend, index=period_list).reindex(get_default_time_line_periods()).applymap(
            lambda x: float(0) if pd.isnull(x) else x)

        print(df_dividend)
        # print(df_dividend.sum(axis=0))
        # print(type(df_dividend.sum(axis=0)))
        dic_dividend = {}
        for year in range(2013, now.year + 1):
            # print('year ', year, ':', df_dividend.loc[pd.Period(str(year)+'Q1'):pd.Period(str(year)+'Q4'), :].sum())
            dic_dividend[pd.Period(year)] = df_dividend.loc[pd.Period(str(year)+'Q1'):pd.Period(str(year)+'Q4'), :].sum()
        print("\n")
        # print(dic_dividend)
        df_dividend = pd.DataFrame(dic_dividend)
        df_dividend = df_dividend.T
        # period_list = list(map(lambda x: pd.Period(x), df_dividend.index.tolist()))
        # df_final = df_dividend.reindex(period_list)
        print('df_dividend = ', df_dividend)
        print('index type =', type(df_dividend.index))
        # print("final = ", df_final)
        # return df_final
        return df_dividend

    def _get_raw_data(self, stock_id, since, to):
        result = self.dividend_policy_fetcher.fetch(
            {'stock_id': stock_id, 'start_year': since - 1911, 'to_year': to - 1911})
        if result.ok is False:
            print('get content fail')
            return
        return result.content


class _DividendPolicyFetcher2(DataFetcher):
    def __init__(self):
        super().__init__('https://mops.twse.com.tw/mops/web/ajax_t05st09_2')

    def fetch(self, params):
        return super().fetch(
            {'encodeURIComponent': 1, 'step': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': params['stock_id'], 'date1': params['start_year'],
             'date2': params['to_year'], 'qryType': 2, 'firstin': 1})
