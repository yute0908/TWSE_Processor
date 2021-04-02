import pandas as pd
import traceback

from bs4 import BeautifulSoup
from idna import unicode

from rdss.fetch_data_utils import PATH_DIR_RAW_DATA_CASH_FLOW, get_raw_data
from rdss.fetcher import DataFetcher
from rdss.statement_processor import StatementProcessor, Source
from repository.mongodb_repository import MongoDBMeta, MongoDBRepository
from utils import get_time_lines


class CashFlowStatementProcessor(StatementProcessor):

    """業主盈餘現金流 = 營業活動之淨現金流入 + 取得不動產、廠房及設備 + 其他投資活動
       自由現金流 = 營業活動之淨現金流入 + 投資活動之淨現金流入"""

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__repository = MongoDBRepository(MongoDBMeta.CASH_FLOW)
        self._fetch_fields = ('營業活動之淨現金流入', '取得不動產、廠房及設備', '其他投資活動', '投資活動之淨現金流入')

    def get_data_frames(self, since, to=None, source_policy=Source.CACHE_ONLY):
        time_lines = get_time_lines(since=since, to=to)
        # time_first = time_lines[0]
        # if time_first.get('season') > 1:
        #     time_lines.insert(0, {'year': time_first.get('year'), 'season': (time_first.get('season') - 1)})
        # print(time_lines)

        time_lines.reverse()

        dfs = []
        cache_data_dict = None
        for time_line in time_lines:
            print('In ', time_line)
            year = time_line.get('year')
            season = time_line.get('season')
            if cache_data_dict is None:
                data_dict = self._get_data_dict(self._fetch_fields, year, season)
            else:
                data_dict = cache_data_dict

            if data_dict is None:
                continue
            if season > 1:
                cache_data_dict = self._get_data_dict(self._fetch_fields, year, season - 1)
                if data_dict is None or cache_data_dict is None:
                    print('get None value in year ', year, ' season ', season, " data_dict = ", data_dict, " cache_data_dic = ", cache_data_dict)
                else:
                    for key in self._fetch_fields:
                        data_dict[key] = data_dict.get(key, 0) - cache_data_dict.get(key, 0)
            else:
                cache_data_dict = None
            data_dict['業主盈餘現金流'] = data_dict.get('營業活動之淨現金流入', 0) + data_dict.get('取得不動產、廠房及設備', 0)\
                                   + data_dict.get('其他投資活動', 0)
            data_dict['自由現金流'] = data_dict.get('營業活動之淨現金流入', 0) + data_dict.get('投資活動之淨現金流入', 0)
            print(data_dict)
            str_period = "{}Q{}".format(year, season)
            period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                          freq='Q')
            dfs.append(pd.DataFrame([data_dict.values()], columns=data_dict.keys(), index=period_index))
        return None if len(dfs) == 0 else pd.concat(dfs, sort=False)

    def get_data_frame(self, year, season, source_policy=Source.CACHE_ONLY):
        return self.get_data_frames(since={'year': year, 'season': season}, to={'year': year, 'season': season},
                                    source_policy=source_policy)

    def _get_data_dict(self, fields, year, season):
        # result = self._data_fetcher.fetch(params={'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        # if result.ok is False:
        #     return None

        data_dict = {}
        try:
            raw_data = self.__repository.get_data(str(self._stock_id), {'year': year, 'season': season})
            raw_data = get_raw_data(PATH_DIR_RAW_DATA_CASH_FLOW + str(year) + "Q" + str(season), str(self._stock_id))
            bs = BeautifulSoup(raw_data, 'html.parser')
            table = bs.find_all('table', attrs={"class": "hasBorder", "align": "center"})
            #print(table[0].prettify())

            rows = table[0].find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                if len(r) == 0:
                    continue
                for field in fields:
                    if field in r[0]:
                        data_dict[field] = int(r[1].replace(',', ''))
                        break

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return None
        # print(data_dict)
        return data_dict


class _CashFlowStatementFetcher(DataFetcher):
    def __init__(self):
        super().__init__("https://mops.twse.com.tw/mops/web/ajax_t164sb05")

    def fetch(self, params):
        return super().fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': params['stock_id'], 'year': params['year'],
             'season': params['season']})
