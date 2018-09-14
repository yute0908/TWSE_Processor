import pandas as pd

from fetcher import DataFetcher
from rdss.parsers import DefaultParser
from rdss.statement_processor import StatementProcessor
from rdss.utility_provider import UtilityProvider


class BalanceSheetProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id,
                         utility_provider=UtilityProvider(DataFetcher('http://mops.twse.com.tw/mops/web/ajax_t164sb03'),
                                                          DefaultParser()))

    def params(self, year, season):
        return {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'queryName': 'co_id',
                'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false', 'co_id': self._stock_id, 'year': year,
                'season': season}


class IncomeStatementProcessor(StatementProcessor):

    def __init__(self, stock_id):
        super().__init__(stock_id,
                         utility_provider=UtilityProvider(DataFetcher('http://mops.twse.com.tw/mops/web/ajax_t164sb04'),
                                                          DefaultParser()))

    def params(self, year, season):
        return {
            'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'queryName': 'co_id', 'TYPEK': 'all',
            'isnew': 'false','co_id': self._stock_id, 'year': year, 'season': season}
