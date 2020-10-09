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
        table = soup.find('table', attrs={"class": "hasBorder", "width": "99%"})

        try:
            data_frame = pd.read_html(str(table))[0]
        except Exception as e:
            print('get', e, ' when get dividend policy')
            return None
        data_frame = data_frame.iloc[3:, :]
        period_list = list(map(lambda x: pd.Period(self.__parse_period(x)), data_frame.iloc[:, 1].tolist()))
        dividend_cash_list = list(map(lambda x: float(x), data_frame.iloc[:, 10].tolist()))
        dividend_cash_stock_list = list(map(lambda x: float(x), data_frame.iloc[:, 13].tolist()))
        dividend_record_version = list(map(lambda x: int(x), data_frame.iloc[:, 3].tolist()))
        meeting_progress = list(map(lambda x: str(x), data_frame.iloc[:, 0].tolist()))
        parse_dict = {}
        for index in range(0, len(period_list)):
            period = period_list[index]
            if parse_dict.get(period) is None:
                parse_dict[period] = [dividend_cash_list[index], dividend_cash_stock_list[index],
                                      dividend_record_version[index]]
            else:
                print('duplicate ', period)
                if meeting_progress[index].find('股東會確認') != -1 and parse_dict[period][2] < dividend_record_version[index]:
                    parse_dict[period] = [dividend_cash_list[index], dividend_cash_stock_list[index],
                                          dividend_record_version[index]]
        period_list = parse_dict.keys()
        dividend_cash_list = [value[0] for value in parse_dict.values()]
        dividend_cash_stock_list = [value[1] for value in parse_dict.values()]
        dict_dividend = {'現金股利': dividend_cash_list, '配股': dividend_cash_stock_list}

        now = datetime.now()

        def get_default_time_line_periods():
            periods = []
            for year in range(2013, now.year + 1):
                for quarter in range(1, 5):
                    periods.append(pd.Period(str(year) + 'Q' + str(quarter)))
            return periods

        df_dividend = pd.DataFrame(dict_dividend, index=period_list).reindex(get_default_time_line_periods()).applymap(
            lambda x: float(0) if pd.isnull(x) else x)
        dic_dividend = {}
        for year in range(2013, now.year + 1):
            dic_dividend[pd.Period(year)] = df_dividend.loc[pd.Period(str(year) + 'Q1'):pd.Period(str(year) + 'Q4'),
                                            :].sum()
        print("\n")
        df_dividend = pd.DataFrame(dic_dividend)
        df_dividend = df_dividend.T
        print('df_dividend = ', df_dividend)
        return df_dividend

    def _get_raw_data(self, stock_id, since, to):
        result = self.dividend_policy_fetcher.fetch(
            {'stock_id': stock_id, 'start_year': since - 1911, 'to_year': to - 1911})
        if result.ok is False:
            print('get content fail')
            return
        return result.content

    def __parse_period(self, period_string):
        if period_string.find("年年度") > -1:
            return str(int(period_string.replace("年年度", "")) + 1911) + "Q4"
        elif period_string.find("年上半年") > -1:
            return str(int(period_string.replace("年上半年", "")) + 1911) + "Q2"
        elif period_string.find("年下半年") > -1:
            return str(int(period_string.replace("年下半年", "")) + 1911) + "Q4"
        else:
            period_strings = period_string.replace("季", "").split("年第")
            return str((int(period_strings[0]) + 1911)) + "Q" + period_strings[1]


class _DividendPolicyFetcher2(DataFetcher):
    def __init__(self):
        super().__init__('https://mops.twse.com.tw/mops/web/ajax_t05st09_2')

    def fetch(self, params):
        return super().fetch(
            {'encodeURIComponent': 1, 'step': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': params['stock_id'], 'date1': params['start_year'],
             'date2': params['to_year'], 'qryType': 2, 'firstin': 1})
