import abc


class Repository(abc.ABC):
    @abc.abstractmethod
    def get_data(self, stock_id, time_line=None):
        return NotImplemented

