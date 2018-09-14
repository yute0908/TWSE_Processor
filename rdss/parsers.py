import abc
import traceback
from datetime import datetime

import pandas as pd


class DataFrameParser(abc.ABC):
    @abc.abstractmethod
    def parse(self, beautiful_soup, year, season):
        return NotImplemented


class DefaultParser(DataFrameParser):
    def parse(self, beautiful_soup, year, season):
        str_date = datetime.strftime(datetime(year + 1911, season * 3, 1), '%Y-%m')
        try:
            table = beautiful_soup.find('table', attrs={"class": "hasBorder", "align": "center"})
            rows = table.find_all('tr')
            print('parse_table:', str_date)
            rows_in_data_frame = self.__get_rows_list(rows)
            processed_rows, row_indexes = self.__get_row_datas(rows_in_data_frame)
            column_indexes = [(str_date, '金額(千元)'), (str_date, '%')]

            return pd.DataFrame(processed_rows, columns=pd.MultiIndex.from_tuples(column_indexes, names=['時間', '金額/百分比']),
                            index=pd.MultiIndex.from_tuples(row_indexes, names=['主要項目', '次要項目']))

        except Exception as inst:
            print("get exception", inst)
            traceback.print_tb(inst.__traceback__)
            return

    def __get_row_datas(self, rows_in_data_frame):
        main_row_index = None
        processed_rows = []
        row_indexes = []
        for row in rows_in_data_frame:
            row_data = ['' if not row[1].strip() else float(row[1].replace(',', '')),
                        '' if not row[2].strip() else float(row[2])]
            #main_row_index = row[0] if (len(row[0]) - len(row[0].lstrip())) == 0 else main_row_index
            main_row_index = row[0].lstrip() if (row_data[0] == '' and row_data[1] == '') else main_row_index
            second_row_index = row[0].lstrip()
            if not (row_data[0] == '' and row_data[1] == ''):
                processed_rows.append(row_data)
                row_indexes.append((main_row_index, second_row_index))
        return processed_rows, row_indexes

    def __get_rows_list(self, rows):
        rows_in_data_frame = []
        for row in rows:
            r = [x.get_text() for x in row.find_all('td')]
            if len(r) > 2:
                rows_in_data_frame.append(r[0: 3])
        return rows_in_data_frame

