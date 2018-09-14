import abc
import pandas as pd

from bs4 import BeautifulSoup

from data_processor import DataProcessor
from utils import get_time_lines


class StatementProcessor(DataProcessor, abc.ABC):

    def __init__(self, stock_id, utility_provider):
        super().__init__(stock_id)
        self._utility_provider = utility_provider

    def get_data_frames(self, since, to=None):
        time_lines = get_time_lines(since=since, to=to)

        dfs = []
        for time_line in time_lines:
            data_frame = self.get_data_frame(time_line.get('year'), time_line.get('season'))
            if data_frame is not None:
                dfs.append(data_frame)

        # return
        return pd.concat(dfs, axis=1, sort=False)

    def get_data_frame(self, year, season):
        result = self._utility_provider.data_fetcher.fetch(self.params(year, season))
        if result.ok is False:
            print('get content fail')
            return
        return self._utility_provider.data_parser.parse(BeautifulSoup(result.content, 'html.parser'), year, season)

    @abc.abstractmethod
    def params(self, year, season):
        return NotImplemented

