import pandas as pd
import traceback

from bs4 import BeautifulSoup

from rdss.parsers import DataFrameParser
from rdss.simple_statments_fetcher import _SimpleBalanceStatementsFetcher
from rdss.statement_processor import StatementProcessor
from utils import get_time_lines


class SimpleIncomeStatementProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__data_fetcher = _SimpleBalanceStatementsFetcher()
        self.__data_parser = _IncomeStatementParser()

    def get_data_frames(self, since, to=None):
        time_lines = get_time_lines(since=since, to=to)
        year = time_lines[0].get('year')
        season = time_lines[0].get('season')
        last_result = self._get_data_dict(year, season - 1) if season > 1 else None
        dfs = []

        for time_line in time_lines:
            data_dict = self._get_data_dict(time_line.get('year'), time_line.get('season'))
            if data_dict is None:
                continue

            if last_result is not None:
                result = {k: (v - last_result[k]) for (k, v) in data_dict.items()}
            else:
                result = data_dict
            print('result = ', result, ' last_result', last_result)

            last_result = None if time_line.get('season') == 4 else data_dict
            str_period = "{}Q{}".format(time_line.get('year'), time_line.get('season'))
            period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                          freq='Q')
            dfs.append(pd.DataFrame([result.values()], columns=result.keys(), index=period_index))

        return pd.concat(dfs) if len(dfs) > 0 else None

    def get_data_frame(self, year, season):
        return self.get_data_frames(since={'year': year, 'season': season}, to={'year': year, 'season': season})

    def _get_data_dict(self, year, season):
        result = self.__data_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        if result.ok is False:
            return None
        try:
            dict_datas = {}
            bs = BeautifulSoup(result.content, 'html.parser')
            tables = bs.find_all('table', attrs={"class": "hasBorder", "align": "center", "width": "70%"})
            table = tables[2]
            rows = table.find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                # print(r)
                if '每股盈餘' in r[0]:
                    dict_datas['EPS'] = float(r[1])
                if '本期綜合損益總額' in r[0]:
                    dict_datas['稅後淨利'] = int(r[1].replace(',', ''))
            return dict_datas

        except Exception as inst:
            print("get exception", inst, " when get data in year ", year, ' and season ', season)
            traceback.print_tb(inst.__traceback__)
            return None


class _IncomeStatementParser(DataFrameParser):
    def parse(self, beautiful_soup, year, season):
        str_period = "{}Q{}".format(year, season)
        dict_datas = {}
        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center", "width": "70%"})
            table = tables[2]
            rows = table.find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                # print(r)
                if '每股盈餘' in r[0]:
                    dict_datas['EPS'] = float(r[1])
                if '本期淨利' in r[0]:
                    dict_datas['稅後淨利'] = int(r[1].replace(',', ''))


        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                      freq='Q')
        return pd.DataFrame([dict_datas.values()], columns=dict_datas.keys(), index=period_index)