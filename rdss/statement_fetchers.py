import traceback

import pandas as pd
from bs4 import BeautifulSoup

from rdss.fetch_data_utils import fetch_simple_balance_sheet_raw_data
from rdss.parsers import DataFrameParser
from rdss.statement_processor import Source
from repository.mongodb_repository import MongoDBRepository, MongoDBMeta
from utils import get_time_lines


class SimpleIncomeStatementProcessor:

    def __init__(self):
        self.__repository = MongoDBRepository(MongoDBMeta.SIMPLE_BALANCE_SHEET)

    def get_data_frames(self, stock_id, since, to=None, source_policy=Source.CACHE_ONLY):
        time_lines = get_time_lines(since=since, to=to)
        year = time_lines[0].get('year')
        season = time_lines[0].get('season')
        last_result = self._get_data_dict(stock_id, year, season - 1) if season > 1 else None
        dfs = []

        for time_line in time_lines:
            data_dict = self._get_data_dict(stock_id, time_line.get('year'), time_line.get('season'))
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

    def get_data_frame(self, stock_id, year, season, source_policy=Source.CACHE_ONLY):
        return self.get_data_frames(stock_id=stock_id, since={'year': year, 'season': season}, to={'year': year, 'season': season},
                                    source_policy=source_policy)

    def _get_data_dict(self, stock_id, year, season):
        # result = self.__data_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        # if result.ok is False:
        #     return None
        try:
            dict_datas = {}
            raw_data = self.__repository.get_data(stock_id, {'year': year, 'season': season})
            if raw_data is None:
                fetch_simple_balance_sheet_raw_data(stock_id, year, season)
                raw_data = self.__repository.get_data(stock_id, {'year': year, 'season': season})
            bs = BeautifulSoup(raw_data, 'html.parser')
            print(' get ', bs.text)
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
