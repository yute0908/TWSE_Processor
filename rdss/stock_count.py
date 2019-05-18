from bs4 import BeautifulSoup

from rdss.fetcher import DataFetcher


class StockCountProcessor:

    def __init__(self):
        self.__data_fetcher = DataFetcher('https://mops.twse.com.tw/mops/web/ajax_t16sn02')

    def get_stock_count(self, stock_id, year):
        result = self.__data_fetcher.fetch(
            {'encodeURIComponent': 1, 'step': 1, 'firstin': 1, 'off': 1, 'queryName': 'co_id', 't05st29_c_ifrs': 'N',
             't05st30_c_ifrs': 'N', 'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false', 'co_id': stock_id,
             'year': (year - 1911)}
        )

        if result.ok is False:
            return None
        bs = BeautifulSoup(result.content, 'html.parser')
        # table01 = bs.find_all('table', {'width': '90%'})
        table = bs.find_all(has_table_width_no_class)
        # print(bs.prettify())
        # print(len(table))
        # print(table[0].prettify())
        rows = table[0].find_all('tr')
        for row in rows:
            r = [x.get_text().strip().replace(" ", "").replace(",", "") for x in row.find_all('td')]
            print(r)

            if len(r) > 3 and r[1] == '合計':
                return int(r[3])

        return 0


def has_table_width_no_class(tag):
    return tag.name == 'table' and tag.has_attr('width') and not tag.has_attr('class')
