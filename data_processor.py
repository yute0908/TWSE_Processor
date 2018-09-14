import abc


class DataProcessor(abc.ABC):

    def __init__(self, stock_id):
        self._stock_id = stock_id

    @abc.abstractmethod
    def get_data_frame(self, year, season):
        return NotImplemented
