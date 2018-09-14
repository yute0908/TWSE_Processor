class UtilityProvider:

    def __init__(self, data_fetcher, data_parser):
        self.__data_fetcher = data_fetcher
        self.__data_parser = data_parser

    @property
    def data_fetcher(self):
        return self.__data_fetcher

    @property
    def data_parser(self):
        return self.__data_parser

