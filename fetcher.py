import requests


class DataFetcher:
    def __init__(self, url):
        self.url = url
        self.session = requests.session()

    def fetch(self, params):
        result = self.session.post(self.url, params)
        return result
