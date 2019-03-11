import pandas as pd
import traceback

from bs4 import BeautifulSoup

from data_processor import DataProcessor
from rdss.fetcher import DataFetcher
from rdss.parsers import DataFrameParser
from rdss.statement_processor import StatementProcessor
from rdss.utils import normalize_params


class DividendPolicyProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__data_fetcher = _DividendPolicyFetcher()
        self.__data_parser = _DividendPolicyParser()

    def get_data_frames(self, since, to=None):
        params = normalize_params(self._stock_id, since.get('year'), to.get('year') if to is not None else None)
        since_year = params.get('since_year')
        to_year = params.get('to_year')
        print(params)
        dfs = []
        for year in range(since_year, to_year + 1):
            df = self.get_data_frame(year)
            if df is not None:
                dfs.append(df)
        return pd.concat(dfs, sort=True) if len(dfs) > 0 else None

    def get_data_frame(self, year, season=None):
        result = self.__data_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911})
        if result.ok is False:
            print('get content fail')
            return
        # print(BeautifulSoup(result.content, 'html.parser').prettify())
        return self.__data_parser.parse(BeautifulSoup(result.content, 'html.parser'), year, season)


class _DividendPolicyFetcher(DataFetcher):

    def __init__(self):
        super().__init__('http://mops.twse.com.tw/mops/web/ajax_t05st09')

    def fetch(self, params):
        return super().fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': params['stock_id'], 'year': params['year']})


class _DividendPolicyParser(DataFrameParser):
    def parse(self, beautiful_soup, year, season):

        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder"})
            if len(tables) > 0:
                row = tables[0].find('tr', attrs={'class': 'odd'})
                items = row.find_all('td')
                dict_datas = {'現金股利': float(items[7].get_text()), '股息': float(items[10].get_text())}
                process_year = int(items[0].get_text()) + 1911
                print(dict_datas)
            else:
                print('No content')
                return

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        period_index = pd.PeriodIndex(start=pd.Period(process_year, freq='Y'), end=pd.Period(process_year, freq='Y'),
                                      freq='Y')
        # print('values = ', [list(dict_datas.values())], ' columns = ', list(dict_datas.keys()), ' index = ', period_index)
        # return pd.DataFrame([list(dict_datas.values())], columns=list(dict_datas.keys()), index=period_index)
        return pd.DataFrame([dict_datas.values()], columns=dict_datas.keys(), index=period_index)