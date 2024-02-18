import pandas as pd
import numpy as np
import xlwings as xw
import os


class FnTools:
    """
    FnGuide 데이터베이스 서비스(DataGuide)를 활용할 수 있도록 엑셀프로그램을 제어하는 클래스

    Parameters
    ----------
    file_name : 데이터가이드 양식과 리프레시 매크로가 포함된 엑셀파일(.xlsm)의 파일명
    
    Attributes
    ----------
    wb : xlwings.Book
        엑셀파일의 workbook 객체
    ws : xlwings.Sheet
        엑셀파일 내 첫번째 worksheet 객체
    """
    def __init__(self, file_name):
        self.path = os.getcwd() + '/util/'
        self.wb = xw.Book(self.path + file_name)
        self.ws = self.wb.sheets(1)

    def close(self):
        """활성화된 workbook 객체를 닫음"""
        self.wb.app.quit()

    def identify_range(self, range_name):
        """
        워크시트 내 특정 셀 또는 범위에 상응하는 Range 객체를 반환함

        Parameters
        ----------
        range_name : str
            워크시트 내 특정 셀(ex. 'A1') 또는 범위(ex. 'A1:B2')의 주소

        Returns
        -------
        xlwings.Range
            입력된 주소에 해당하는 Range 객체
        """

        if isinstance(range_name, xw.Range):
            return range_name
        else:
            return self.ws[range_name]

    def clear_table(self, base_cell):
        """
        특정 셀을 좌상단으로 하는 테이블 영역 내의 모든 값과 서식을 삭제

        Parameters
        ----------
        base_cell : str
            기준이 되는 셀 주소
        """
        self.identify_range(base_cell).expand('table').clear()

    def paste(self, base_cell, value, down=False):
        value = value.values if isinstance(value, (pd.Series, pd.Index, pd.DatetimeIndex, pd.DataFrame)) \
            else np.array(value)
        if down:
            value = value.reshape(-1, 1)
        self.identify_range(base_cell).options(np.ndarray).value = value

    def to_df(self, base_cell='A1', cols=None):
        return self.identify_range(base_cell).options(pd.DataFrame, expand='table', index=False).value

    def refresh(self):
        self.wb.macro("DoAllSheetRefresh")()
