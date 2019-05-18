import time

import requests
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import NewConnectionError, MaxRetryError, ProtocolError


class DataFetcher:
    _fetch_count = 0
    _backoff_window_count = 0

    def __init__(self, url):
        self.url = url
        self.session = requests.session()

    def fetch(self, params):
        self.wait_for_server()
        get_value = False
        while get_value is not True:
            try:
                print('fetch ', self.url, ' params = ', params)
                result = self.session.post(self.url, params, headers={'Connection':'close'}, timeout=60)
                get_value = True
                print('fetch success')
            except (requests.exceptions.ConnectionError, ConnectionRefusedError, NewConnectionError, MaxRetryError,
                    ProtocolError) as ce:
                print('get connection error ce ', ce)
                self.wait_for_server()
            except (ChunkedEncodingError, ProtocolError) as ce2:
                print('get connection error 2 ce ', ce2)

                self.wait_for_server()

        return result

    @staticmethod
    def wait_for_server():
        time.sleep(5)
        # DataFetcher._fetch_count += 1
        # if DataFetcher._fetch_count % 5 == 0:
        #     sleep_time = 5 * (DataFetcher._backoff_window_count % 6 + 1)
        #     DataFetcher._backoff_window_count += 1
        #     time.sleep(sleep_time)

