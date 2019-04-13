import time

import requests


class DataFetcher:
    _fetch_count = 0
    _backoff_window_count = 0

    def __init__(self, url):
        self.url = url
        self.session = requests.session()

    def fetch(self, params):
        self.wait_for_server()
        result = self.session.post(self.url, params, headers={'Connection':'close'})
        return result

    @staticmethod
    def wait_for_server():
        time.sleep(5)
        # DataFetcher._fetch_count += 1
        # if DataFetcher._fetch_count % 5 == 0:
        #     sleep_time = 5 * (DataFetcher._backoff_window_count % 6 + 1)
        #     DataFetcher._backoff_window_count += 1
        #     time.sleep(sleep_time)

