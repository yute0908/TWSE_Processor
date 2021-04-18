import pandas as pd
import traceback

from bs4 import BeautifulSoup

from rdss.fetch_data_utils import fetch_shareholder_equity_raw_data
from rdss.fetcher import DataFetcher
from rdss.statement_processor import StatementProcessor, Source
from repository.mongodb_repository import MongoDBRepository, MongoDBMeta
from utils import get_time_lines


class ShareholderEquityProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__tag = "ShareholderEquityProcessor"
        self.__repository = MongoDBRepository(MongoDBMeta.SHARE_HOLDER)
        # self._data_fetcher = _ShareholderEquityFetcher()
        self.items_to_get = ('期初餘額', '期末餘額')
        self.fields_to_get = ('權益總額',)

    def get_data_frames(self, since, to=None, source_policy=Source.CACHE_ONLY):
        time_lines = get_time_lines(since=since, to=to)
        dfs = []
        column_index = pd.MultiIndex.from_product([self.fields_to_get, self.items_to_get], names=['first', 'second'])
        print(column_index)
        last_result = self._get_data_dict(time_lines[0].get('year'), time_lines[0].get('season')) if len(
            time_lines) > 0 and time_lines[0].get('season') > 1 else None

        for time_line in time_lines:
            result = self._get_data_dict(time_line.get('year'), time_line.get('season'))
            if result is None:
                continue
            if last_result is not None:
                for key in result.keys():
                    result[key]['期初餘額'] = last_result[key]['期末餘額']
            last_result = result
            print(result)
            str_period = "{}Q{}".format(time_line.get('year'), time_line.get('season'))
            period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                          freq='Q')
            data_list = []
            for inner in result.values():
                data_list.extend(inner.values())
            print(data_list)
            dfs.append(pd.DataFrame([data_list], columns=column_index, index=period_index))

        # return super().get_data_frames(since, to)
        print(self.__tag, "dfs = ", dfs)
        return pd.concat(dfs) if len(dfs) > 0 else None

    def get_data_frame(self, year, season):
        return self.get_data_frames(since={'year': year, 'season': season}, to={'year': year, 'season': season})

    def _get_data_dict(self, year, season):
        raw_data = self.__repository.get_data(self._stock_id, {'year': year, 'season': season})
        if raw_data is None:
            fetch_shareholder_equity_raw_data(self._stock_id, year, season)
            raw_data = self.__repository.get_data(self._stock_id, {'year': year, 'season': season})
        if raw_data is not None:
            return self._parse_data(raw_data)

    def _parse_data(self, content):
        try:
            bs = BeautifulSoup(content, 'html.parser')
            # print(bs.prettify())
            tables = bs.find_all('table', attrs={"class": "hasBorder", "align": "center"})

            if len(tables) < 1:
                print('ShareholderEquityProcessor - error 1')

                return None

            table = tables[0]
            # print(table.prettify())
            rows = table.find_all('tr')

            headers = []
            rows_data = []
            for row in rows:
                columns_raw = [column for column in row.contents if column != '\n']
                columns = [column.get_text().strip() for column in columns_raw]
                if len(columns) > 1:
                    if columns_raw[0].name == 'th' and len(headers) == 0:
                        headers = columns
                        if not all(field in headers for field in self.fields_to_get):
                            print('ShareholderEquityProcessor - error 2')

                            return None
                    else:
                        rows_data.append(columns)

            rows_data = [row_data for row_data in rows_data if row_data[0] in self.items_to_get]
            result = {row_data[0]: {k: int(row_data[headers.index(k)].replace(',', '')) for k in self.fields_to_get} for
                      row_data in rows_data}
            result2 = {key: {item: 0 for item in self.items_to_get} for key in self.fields_to_get}
            for key in result.keys():
                for key2 in result[key]:
                    result2[key2][key] = result[key][key2]

            print("result = ", result2)
            return result2

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return None


class _ShareholderEquityFetcher(DataFetcher):
    def __init__(self):
        super().__init__("https://mops.twse.com.tw/mops/web/ajax_t164sb06")
