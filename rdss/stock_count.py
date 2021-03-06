from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from rdss.fetch_data_utils import fetch_stock_count_raw_data
from repository.mongodb_repository import MongoDBRepository, MongoDBMeta


class StockCountProcessor:

    def __init__(self):
        self.__repository = MongoDBRepository(MongoDBMeta.STOCK_COUNT)

    def get_stock_count(self, stock_id, year):
        raw_data = self.__repository.get_data(stock_id, {'year': year})
        if raw_data is None:
            fetch_stock_count_raw_data(stock_id, year, year)
            raw_data = self.__repository.get_data(stock_id, {'year': year})
        if raw_data is None:
            return None
        bs = BeautifulSoup(raw_data, 'html.parser')
        table = bs.find_all(has_table_width_no_class)
        # print(bs.prettify())
        # print(len(table))
        # print(table[0].prettify())
        if len(table) == 0:
            return None
        rows = table[0].find_all('tr')
        for row in rows:
            r = [x.get_text().strip().replace(" ", "").replace(",", "") for x in row.find_all('td')]
            print(r)

            if len(r) > 3 and r[1] == '合計':
                return int(r[3])

        return 0

    def get_data_frame(self, stock_id, since, to=None):
        if to is None or to < since:
            to = datetime.now().year
        stocks = []
        end_year = since
        start_year = since
        for year in range(since, to + 1):
            stock_count = self.get_stock_count(stock_id, year)
            print("StockCountProcessor year = ", year, " stocks = ", stock_count)

            if stock_count is None:
                if start_year == year:
                    start_year = start_year + 1
                    continue
                else:
                    if len(stocks) > 0:
                        break
                    else:
                        return
            stocks.append(stock_count)
            end_year = year
        period_index = pd.PeriodIndex(start=pd.Period(start_year, freq='Y'), end=pd.Period(end_year, freq='Y'), freq='Y')
        return pd.DataFrame(data={'股數': stocks}, index=period_index)




def has_table_width_no_class(tag):
    return tag.name == 'table' and tag.has_attr('width') and not tag.has_attr('class')
