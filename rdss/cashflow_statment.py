from fetcher import DataFetcher
from rdss.statement_processor import StatementProcessor
from utils import get_time_lines


class CashFlowStatementProcessor(StatementProcessor):

    def get_data_frames(self, since, to=None):
        time_lines = get_time_lines(since=since, to=to)

        return super().get_data_frames(since, to)

    def _get_data_frame(self, year, season):
        pass

    def get_data_frame(self, year, season):
        pass


class CashFlowStatementFetcher(DataFetcher):
    def __init__(self):
        super().__init__("http://mops.twse.com.tw/mops/web/ajax_t164sb05")

    def fetch(self, params):
        return super().fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 'inpuType': 'co_id',
             'TYPEK': 'all', 'isnew': 'false', 'co_id': '2330', 'year': 107, 'season': 2})
