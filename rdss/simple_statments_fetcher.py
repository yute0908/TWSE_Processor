from rdss.fetcher import DataFetcher


class _SimpleBalanceStatementsFetcher(DataFetcher):

    def __init__(self):
        super().__init__('https://mops.twse.com.tw/mops/web/ajax_t163sb01')

    def fetch(self, params):

        return super().fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id", "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": params['stock_id'], "year": params['year'],
             "season": params['season']})


class _BalanceStatementsFetcher(DataFetcher):

    def __init__(self):
        super().__init__('https://mops.twse.com.tw/mops/web/ajax_t164sb03')

    def fetch(self, params):
        return super().fetch(
            {"encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1, "queryName": "co_id", "inpuType": "co_id",
             "TYPEK": "all", "isnew": "false", "co_id": params['stock_id'], "year": params['year'],
             "season": params['season']})

    def fetch_second_step_2(self, params):
        return super().fetch(
            {"encodeURIComponent": 1, "step": 2, "firstin": 1, "TYPEK": "sii", "co_id": params['stock_id'],
             "year": params['year'], "season": params['season']}
        )
