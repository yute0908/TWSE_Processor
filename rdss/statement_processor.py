import abc
import pandas as pd

from data_processor import DataProcessor
from utils import get_time_lines


class StatementProcessor(DataProcessor, abc.ABC):

    def get_data_frames(self, since, to=None):
        time_lines = get_time_lines(since=since, to=to)

        dfs = []
        for time_line in time_lines:
            data_frame = self.get_data_frame(time_line.get('year'), time_line.get('season'))
            if data_frame is not None:
                dfs.append(data_frame)

        # return
        return pd.concat(dfs, sort=True)

    @abc.abstractmethod
    def get_data_frame(self, year, season):
        return NotImplemented

