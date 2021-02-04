import abc
from enum import Enum

import pandas as pd

from data_processor import DataProcessor
from utils import get_time_lines


class Source(Enum):
    CACHE_ONLY = 'CACHE_ONLY'
    REMOTE_IF_NO_CACHE = 'REMOTE_IF_NO_CACHE'


class StatementProcessor(DataProcessor, abc.ABC):

    def get_data_frames(self, since, to=None, source_policy=Source.CACHE_ONLY):
        time_lines = get_time_lines(since=since, to=to)

        dfs = []
        for time_line in time_lines:
            data_frame = self.get_data_frame(time_line.get('year'), time_line.get('season'), source_policy)
            if data_frame is not None:
                dfs.append(data_frame)

        # return
        return pd.concat(dfs, sort=True) if len(dfs) > 0 else None

    @abc.abstractmethod
    def get_data_frame(self, year, season, source_policy=Source.CACHE_ONLY):
        return NotImplemented
