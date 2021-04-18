import traceback
from functools import reduce

import pandas as pd
from bs4 import BeautifulSoup

from rdss.fetch_data_utils import fetch_simple_balance_sheet_raw_data, fetch_balance_sheet_raw_data
from rdss.parsers import DataFrameParser
from rdss.statement_processor import StatementProcessor, Source
from repository.mongodb_repository import MongoDBRepository, MongoDBMeta


class SimpleBalanceSheetProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id)
        self.__simple_balance_repository = MongoDBRepository(MongoDBMeta.SIMPLE_BALANCE_SHEET)
        self.__full_balance_repository = MongoDBRepository(MongoDBMeta.FULL_BALANCE_SHEET)
        self.__data_parser = _SimpleBalanceSheetParser()

    def get_data_frame(self, year, season, source_policy=Source.CACHE_ONLY):
        def dict_generator(raw_data, parser):
            return None if raw_data is None else parser(raw_data)

        simple_balance_sheet_raw_data = self.__simple_balance_repository.get_data(self._stock_id, {'year': year, 'season': season})
        if simple_balance_sheet_raw_data is None:
            fetch_simple_balance_sheet_raw_data(self._stock_id, year, season)
            simple_balance_sheet_raw_data = self.__simple_balance_repository.get_data(self._stock_id, {'year': year, 'season': season})

        balance_sheet_raw_data = self.__full_balance_repository.get_data(self._stock_id, {'year': year, 'season': season})
        if balance_sheet_raw_data is None:
            fetch_balance_sheet_raw_data(self._stock_id, year, season)
            balance_sheet_raw_data = self.__full_balance_repository.get_data(self._stock_id, {'year': year, 'season': season})

        dict_simple_balance_sheet = dict_generator(simple_balance_sheet_raw_data, self.__parse_simple_balance_sheet)
        dict_balance_sheet = dict_generator(balance_sheet_raw_data, self.__parse_full_balance_sheet)
        if dict_simple_balance_sheet is None or dict_balance_sheet is None:
            print('SimpleBalanceSheetProcessor - get_data_frame year = ', year, ' season = ', season, ' result is null')
            return None
        else:
            dict_balance_sheet.update(dict_simple_balance_sheet)
            str_period = "{}Q{}".format(year, season)
            period_index = pd.PeriodIndex(start=pd.Period(str_period, freq='Q'), end=pd.Period(str_period, freq='Q'),
                                          freq='Q')
            print('SimpleBalanceSheetProcessor - get_data_frame year = ', year, ' season = ', season, ' result is not null')
            return pd.DataFrame([dict_balance_sheet.values()], columns=dict_balance_sheet.keys(), index=period_index)

    def __parse_simple_balance_sheet(self, raw_input):
        table = self.get_simple_balance_sheet_table(raw_input)
        dict_datas = {}
        try:
            rows = table.find_all('tr')
            for row in rows:
                r = [x.get_text() for x in row.find_all('td')]
                if '每股淨值' in r[0]:
                    dict_datas['每股淨值'] = float(r[1])

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

        return dict_datas

    def get_simple_balance_sheet_table(self, raw_input):
        try:
            tables = BeautifulSoup(raw_input, 'html.parser').find_all('table',
                                                                      attrs={"class": "hasBorder", "align": "center",
                                                                             "width": "70%"})
            return tables[1]
        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)

    def __parse_full_balance_sheet(self, raw_input):
        table = self.get_balance_sheet_table(raw_input)
        dict_datas = {}
        fields_long_term_investment = ['採用權益法之投資', '採用權益法之投資淨額', '透過損益按公允價值衡量之金融資產－非流動', '持有至到期日金融資產－非流動淨額',
                                       '以成本衡量之金融資產－非流動淨額']
        fields_property = ['不動產、廠房及設備', '不動產、廠房及設備淨額', '不動產及設備合計', '不動產及設備－淨額']
        try:
            rows = table.find_all('tr')
            trs = [[x.get_text().strip() for x in row.find_all('td')] for row in rows]
            record_long_term_investment = list(
                filter(lambda tr: len(tr) > 0 and tr[0] in fields_long_term_investment, trs))
            record_property = list(filter(lambda tr: len(tr) > 0 and tr[0] in fields_property, trs))
            print('record_long_term_investment = ', record_long_term_investment)
            dict_datas['長期投資'] = reduce(lambda result, element: result + element,
                                        [int(values[1].replace(",", '')) for values in record_long_term_investment],
                                        0) if len(record_long_term_investment) > 0 else 0
            dict_datas['固定資產'] = int(record_property[0][1].replace(",", '')) if len(record_property) > 0 else 0
            return dict_datas
        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

    def get_balance_sheet_table(self, raw_input):
        beautiful_soup = BeautifulSoup(raw_input, 'html.parser')
        try:
            has_data = not (len(
                beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center", "width": "50%"})) > 0)
            if has_data:
                tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center"})
                return tables[0]
            else:
                return None
        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

    def parse_balance_sheet(self, beautiful_soup, year, season):
        str_period = "{}Q{}".format(year, season)
        dict_datas = {}
        fields_long_term_investment = ['採用權益法之投資', '採用權益法之投資淨額', '透過損益按公允價值衡量之金融資產－非流動', '持有至到期日金融資產－非流動淨額']
        fields_assets = ['不動產、廠房及設備', '不動產、廠房及設備淨額', '不動產及設備合計']
        fields = fields_long_term_investment + fields_assets
        try:
            tables = beautiful_soup.find_all('table', attrs={"class": "hasBorder", "align": "center"})
            table = tables[0]
            # print('table = ', table.prettify())
            rows = table.find_all('tr')
            trs = [[x.get_text().strip() for x in row.find_all('td')] for row in rows]
            record_long_term_investment = list(
                filter(lambda tr: len(tr) > 0 and tr[0] in fields_long_term_investment, trs))
            record_property = list(filter(lambda tr: len(tr) > 0 and tr[0] in fields_assets, trs))
            print('record_long_term_investment = ', record_long_term_investment)
            # filtered_trs = filter(lambda tr: len(tr) > 0, trs)
            # for row in rows:
            #     tr = [x.get_text().strip() for x in row.find_all('td')]
            #     print("tr = ", tr)
            # rows = table.find_all('tr')
            # for row in rows:
            #     r = [x.get_text() for x in row.find_all('td')]
            #     if '每股淨值' in r[0]:
            #         dict_datas['每股淨值'] = float(r[1])
            # return list(filtered_trs)
            dict_datas['長期投資'] = record_long_term_investment[0] if (len(record_long_term_investment) > 0) else 0
            dict_datas['固定資產'] = record_property[0] if (len(record_property) > 0) else 0
            return dict_datas
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
