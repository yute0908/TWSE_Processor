import json
import logging
import os
import traceback
from datetime import datetime
from enum import Enum
from urllib.parse import urlencode

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxProfile

from data_processor import DataProcessor
from rdss.fetch_data_utils import mongo_client, DB_TWSE, TABLE_TPEX_PRICE_MEASUREMENT, DB_TWSE_DATAFRAMES, \
    TABLE_DATAFRAME_PRICE_MEASUREMENT, TABLE_TWSE_PRICE_MEASUREMENT, __logger, fetch_twse_price_measurement_raw_datas, \
    fetch_tpex_price_measurement_raw_datas
from repository.mongodb_repository import MongoDBRepository, MongoDBMeta, Transformer, default_data_frame_out_transform, \
    yearly_period_data_frame_in_transform
from twse_crawler import gen_output_path


_logger = logging.getLogger("twse.DataFetcher")

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


def __data_frame_in_transform(content):
    data_frame = pd.read_json(content, orient='split', typ='frame')
    print(data_frame.index.values)
    index_dict = {item: pd.Period(value=str(item)) for item in data_frame.index.values}
    new_data_frame = data_frame.rename(index_dict)
    return new_data_frame


__data_frame_repository_transformer = Transformer(in_transform=yearly_period_data_frame_in_transform,
                                                  out_transform=default_data_frame_out_transform)
_data_frame_repository = MongoDBRepository(MongoDBMeta.DATAFRAME_PRICE_MEASUREMENT,
                                           transformer=__data_frame_repository_transformer)

class PriceMeasurementProcessor2:
    def __init__(self):
        self.__twsePriceTransformer = TWSEPriceMeasurementTransformer()
        self.__tpexPriceTransformer = TPEXPriceMeasurementTransformer()
        from evaluation_utils import get_stock_codes
        self.list_twse = get_stock_codes(stock_type='上市')
        self.list_tpex = get_stock_codes(stock_type='上櫃')

    def get_data_frame(self, stock_id):
        data_frame = _data_frame_repository.get_data(stock_id)
        current_years = None
        if data_frame is not None:
            current_years = list(map(lambda year_index: int(year_index.year), data_frame.index.values))
            current_years.sort()
        print('data_frame before = ', data_frame)
        if data_frame is None or (current_years is not None and current_years[-1] < (datetime.now().year - 1)):
            if stock_id in self.list_twse:
                fetch_twse_price_measurement_raw_datas([stock_id])
                data_frame = self.__twsePriceTransformer.transform_to_dataframe(stock_id=stock_id)
            if stock_id in self.list_tpex:
                fetch_tpex_price_measurement_raw_datas([stock_id])
                data_frame = self.__tpexPriceTransformer.transform_to_dataframe(stock_id=stock_id)

        print('data_frame after = ', data_frame)
        return data_frame

class TWSEPriceMeasurementTransformer:
    def __init__(self):
        self.__in_repository = MongoDBRepository(MongoDBMeta.TWSE_PRICE_MEASUREMENT)
        self.__out_repository = _data_frame_repository

    def transform_to_dataframe(self, stock_id):
        content = self.__in_repository.get_data(stock_id)
        # print(content['fields'])
        if content is None:
            return
        rows = []
        indexes = []
        _logger.info("TWSEPriceMeasurementTransformer transform " + str(stock_id))
        for row_items in content['data']:
            row = [str(row_item).replace(',', '') for row_item in row_items]
            row[1] = int(row[1])
            row[2] = int(row[2])
            row[3] = int(row[3])
            row[4] = float(row[4])
            row[6] = float(row[6])
            row[8] = float(row[8])
            indexes.append(pd.Period(value=str(int(row[0]) + 1911)))
            rows.append(row[1:])
        data_frame = pd.DataFrame(rows, index=indexes,
                                  columns=['成交股數', '成交金額', '成交筆數', '最高價', '日期', '最低價', '日期', '收盤平均價'])
        print(data_frame)
        self.__out_repository.put_data(stock_id, data_frame)
        return data_frame
        # db = mongo_client[DB_TWSE]
        # collection = db[TABLE_DATAFRAME_PRICE_MEASUREMENT]
        # collection.find_one_and_update({'stock_id': str(stock_id)}, {'$set': {"data_frame": data_frame_json}},
        #                        upsert=True)
        # record = collection.find_one({"stock_id": str(stock_id)})
        # data_frame_json = record['data_frame']
        # print(data_frame_json)
        # data_frame_2 = pd.read_json(data_frame_json, orient='split', typ='frame')
        # print(data_frame_2)


class TPEXPriceMeasurementTransformer:
    def __init__(self):
        self.__in_repository = MongoDBRepository(MongoDBMeta.TPEX_PRICE_MEASUREMENT)
        self.__out_repository = _data_frame_repository

    def transform_to_dataframe(self, stock_id):
        # collection = db[TABLE_TPEX_PRICE_MEASUREMENT]
        # record = collection.find_one({"stock_id": str(stock_id)})
        record = self.__in_repository.get_data(stock_id)
        _logger.info("TWSEPriceMeasurementTransformer transform " + str(stock_id))
        if record is not None:
            try:
                soup = BeautifulSoup(record, 'html.parser')
                table = soup.find('table', attrs={"class": "page-table-board"})
                rows = []
                indexes = []
                for tr in table.find_all('tr'):
                    if tr.find('td', attrs={"class": "page-table-body-center"}) is not None:
                        tds = tr.find_all('td')
                        row = [td.string.replace(',', '') for td in tds]
                        row[0] = int(row[0]) + 1911
                        row[1] = int(row[1]) * 1000
                        row[2] = int(row[2]) * 1000
                        row[3] = int(row[3]) * 1000
                        row[4] = float(row[4])
                        row[6] = float(row[6])
                        row[8] = float(row[8])
                        indexes.append(pd.Period(str(row[0])))
                        rows.append(row[1:])
                data_frame = pd.DataFrame(rows, index=indexes,
                                          columns=['成交股數', '成交金額', '成交筆數', '最高價', '日期', '最低價', '日期', '收盤平均價'])
                print(data_frame)
                self.__out_repository.put_data(stock_id, data_frame)
                return data_frame

            except Exception as inst:
                _logger.error("get exception in " + str(stock_id) + ":" + str(inst))
                traceback.print_tb(inst.__traceback__)
            # db = mongo_client[DB_TWSE]
            # collection = db[TABLE_DATAFRAME_PRICE_MEASUREMENT]
            # collection.find_one_and_update({'stock_id': str(stock_id)}, {'$set': {"data_frame": data_frame_json}},
            #                                upsert=True)
            # record = collection.find_one({"stock_id": str(stock_id)})
            # data_frame_json = record['data_frame']
            # print(data_frame_json)
            # data_frame_2 = pd.read_json(data_frame_json, orient='split', typ='frame')
            # print(data_frame_2)
