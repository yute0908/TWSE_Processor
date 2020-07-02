import pandas as pd
import traceback

from bs4 import BeautifulSoup

from rdss.parsers import DataFrameParser
from rdss.simple_statments_fetcher import _SimpleBalanceStatementsFetcher, _BalanceStatementsFetcher
from rdss.statement_processor import StatementProcessor


class SimpleBalanceSheetProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__simple_data_fetcher = _SimpleBalanceStatementsFetcher()
        self.__balance_sheet_fetcher = _BalanceStatementsFetcher()
        self.__data_parser = _SimpleBalanceSheetParser()

    def get_data_frame(self, year, season):
        result = self.__simple_data_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        if result.ok is False:
            print('get content fail')
            return
        result2 = self.__balance_sheet_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        self.__parse_balance_sheet(BeautifulSoup(result2.content, 'html.parser'), year, season)
        # return self.__data_parser.parse(BeautifulSoup(result.content, 'html.parser'), year, season)
        return self.__parse_simple_balance_sheet(BeautifulSoup(result.content, 'html.parser'), year, season)

    def get_raw_data(self, year, season):
        result = self.__balance_sheet_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        if result.ok is False:
            return
        return result.content

    def get_balance_sheet(self, year, season):
        result2 = self.__balance_sheet_fetcher.fetch({'stock_id': self._stock_id, 'year': year - 1911, 'season': season})
        if result2.ok is False:
            return
        return self.__parse_balance_sheet(BeautifulSoup(result2.content, 'html.parser'), year, season)

    def __parse_simple_balance_sheet(self, beautiful_soup, year, season):
        str_period = "{}Q{}".format(year, season)
        dict_datas = {}
        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center", "width": "70%"})
            table = tables[1]
            rows = table.find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                if '每股淨值' in r[0]:
                    dict_datas['每股淨值'] = float(r[1])

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                      freq='Q')
        return pd.DataFrame([dict_datas.values()], columns=dict_datas.keys(), index=period_index)

    def __parse_balance_sheet(self, beautiful_soup, year, season):
        str_period = "{}Q{}".format(year, season)
        dict_datas = {}
        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center"})
            table = tables[0]
            # print('table = ', table.prettify())
            rows = table.find_all('tr')
            trs = [[x.get_text().strip() for x in row.find_all('td')] for row in rows]
            # new_trs = filter(lambda tr: len(tr) > 0 and tr[0] in ['採用權益法之投資', '採用權益法之投資淨額', '不動產、廠房及設備'], trs)
            filtered_trs = filter(lambda tr: len(tr) > 0, trs)
            # for row in rows:
            #     tr = [x.get_text().strip() for x in row.find_all('td')]
            #     print("tr = ", tr)
            # rows = table.find_all('tr')
            # for row in rows:
            #     r = [x.get_text() for x in row.find_all('td')]
            #     if '每股淨值' in r[0]:
            #         dict_datas['每股淨值'] = float(r[1])
            return list(filtered_trs)

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return


class _SimpleBalanceSheetParser(DataFrameParser):
    def parse(self, beautiful_soup, year, season):
        str_period = "{}Q{}".format(year, season)
        dict_datas = {}
        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center", "width": "70%"})
            table = tables[1]
            rows = table.find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                if '每股淨值' in r[0]:
                    dict_datas['每股淨值'] = float(r[1])

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                      freq='Q')
        return pd.DataFrame([dict_datas.values()], columns=dict_datas.keys(), index=period_index)
