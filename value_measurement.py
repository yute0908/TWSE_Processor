import os
import traceback
from enum import Enum
from urllib.parse import urlencode

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxProfile

from data_processor import DataProcessor
from rdss.fetch_data_utils import mongo_client, DB_TWSE, TABLE_TPEX_PRICE_MEASUREMENT
from twse_crawler import gen_output_path


class IndexType(Enum):
    INT_INDEX = 'int'
    YEAR_INDEX = "year_index"


class PriceMeasurementProcessor(DataProcessor):
    def __init__(self, stock_id):
        super().__init__(stock_id)

    def get_data_frame(self, year=None, season=None, indexType=IndexType.INT_INDEX):
        # create a new Firefox session
        path = os.path.abspath(gen_output_path("data"))
        print("path = ", path)

        params = {'STOCK_ID': self._stock_id}
        url = "https://goodinfo.tw/StockInfo/StockBzPerformance.asp?" + urlencode(params)
        print('url = ', url)
        profile = FirefoxProfile()
        profile.set_preference("browser.download.panel.shown", False)
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.dir", path)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/html")
        driver = webdriver.Firefox(firefox_profile=profile)
        driver.get(url)
        elements = driver.find_elements_by_id("txtFinDetailData")
        buttons = elements[0].find_elements_by_xpath(".//input")
        buttons[5].click()
        driver.quit()
        file_path = os.path.join(path, "BzPerformance.html")
        print("path2 = ", file_path)
        try:
            df = pd.read_html(file_path)[0]
            years = self.__correct_years(df.loc[:, 0].tolist())
            prices = list(map(lambda price: float(price) if price != '-' else float(0), df.loc[:, 4].tolist()))
            if indexType == IndexType.INT_INDEX:
                data = {'年度': years, '平均股價': prices}
                df = pd.DataFrame(data)
                df = df.set_index(['年度'])
            else:
                # periodIndexes = list(map(lambda year: pd.PeriodIndex(start=pd.Period(year, freq='Y'),
                #                                                      end=pd.Period(year, freq='Y'),
                #                                                      freq='Y'), years))
                # print('periodIndex = ', periodIndexes)
                periodIndex = pd.PeriodIndex(start=pd.Period(years[-1], freq='Y'), end=pd.Period(years[0], freq='Y'),
                                             freq='Y')
                print(periodIndex)
                print([prices[i:i + 1] for i in range(0, len(prices))])
                df = pd.DataFrame(data=[prices[i:i + 1] for i in range(len(prices) - 1, -1, -1)], columns=['平均股價'],
                                  index=periodIndex)

            print('df = ', df)
            # df['平均股價'] = df['平均股價'].map(lambda price: float(price) if price != '-' else float(0))
            # df2.index = pd.PeriodIndex(df2.index, freq='A')

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return None
        finally:
            os.remove(file_path)
        return df

    def __correct_years(self, year_list):
        revamp_list = []
        for element in year_list:
            try:
                revamp_list.append(int(element))
            except ValueError:
                revamp_list.append(None)
        index_list_not_none = [i for i, element in enumerate(revamp_list) if element is not None]
        print(index_list_not_none)
        for i in range(index_list_not_none[0] - 1, -1, -1):
            revamp_list[i] = revamp_list[i + 1] + 1
        print(revamp_list)
        for i in range(index_list_not_none[0] + 1, len(revamp_list)):
            revamp_list[i] = revamp_list[i - 1] - 1
        print(revamp_list)

        return revamp_list


class TPEXPriceMeasurementProcessor:
    def get_data_frame(self, stock_id):
        db = mongo_client[DB_TWSE]
        collection = db[TABLE_TPEX_PRICE_MEASUREMENT]
        record = collection.find_one({"stock_id": str(1240)})
        if record is not None:
            # print(record['content'])
            soup = BeautifulSoup(record['content'], 'html.parser')
            table = soup.find('table', attrs={"class": "page-table-board"})
            print(table)