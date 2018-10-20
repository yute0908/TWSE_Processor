from fetcher import DataFetcher


class _SimpleStatementsFetcher(DataFetcher):

    def __init__(self):
        super().__init__('http://mops.twse.com.tw/mops/web/ajax_t163sb01')

    def fetch(self, params):

        return super().fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id", "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": params['stock_id'], "year": params['year'],
             "season": params['season']})
