import pandas as pd
import traceback

from bs4 import BeautifulSoup

from rdss.parsers import DataFrameParser
from rdss.simple_statments_fetcher import _SimpleStatementsFetcher
from rdss.statement_processor import StatementProcessor


class SimpleIncomeStatementProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__data_fetcher = _SimpleStatementsFetcher()
        self.__data_parser = _IncomeStatementParser()

    def get_data_frame(self, year, season):
        result = self.__data_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        if result.ok is False:
            print('get content fail')
            return
        return self.__data_parser.parse(BeautifulSoup(result.content, 'html.parser'), year, season)


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

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                      freq='Q')
        return pd.DataFrame([dict_datas.values()], columns=dict_datas.keys(), index=period_index)
