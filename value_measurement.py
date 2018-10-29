import os
from urllib.parse import urlencode

import pandas as pd
from selenium import webdriver
from selenium.webdriver import FirefoxProfile

from data_processor import DataProcessor
from twse_crawler import gen_output_path


class ValueMeasurementProcessor(DataProcessor):
    def __init__(self, stock_id):
        super().__init__(stock_id)

    def get_data_frame(self, year=None, season=None):
        # create a new Firefox session
        path = os.path.abspath(gen_output_path("data"))
        print("path = ", path)

        params = {'STOCK_ID': self._stock_id}
        url = "https://goodinfo.tw/StockInfo/StockBzPerformance.asp?" + urlencode(params)
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
        df = pd.read_html(file_path)[0]

        df2 = df[[0, 4]]
        df2 = df2.drop([0])
        df2 = df2.rename(columns={0: '年度', 4: '平均股價'})
        df2 = df2.set_index(['年度'])
        df2.index = pd.to_datetime(df2.index, format='%Y')
        print(df2.index)

        os.remove(file_path)
        return df2