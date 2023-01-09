import util as ut
import pandas as pd
import numpy as np
import xlwings as xw
import functools
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
        self.path = os.path.dirname(os.path.dirname(__file__)) + '/util/'
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

    def to_df(self, base_cell='A1'):
        return self.identify_range(base_cell).options(pd.DataFrame, expand='table', index=False).value

    def refresh(self):
        self.wb.macro("DoAllSheetRefresh")()


def get_cross_section_data(symbols, fs=None, daily=None, item=None, new=False):
    """
    fs    : 기업재무제표의 경우 [연, 월] list,
    daily : 주식일별지표의 경우 8자리 연월일 str
    """
    symbols = 'A' + pd.Series(symbols)
    table = 'fs' if fs else 'daily'

    if item:
        item_name, item_kind, item_code, item_name_k, item_freq = ut.get_table_info(item=item, new=new)
    else:

        item_name, item_kind, item_code, item_name_k, item_freq = ut.get_table_info(table, new=new)

    ft = FnTools('cs_data.xlsm')
    ft.paste('A7', symbols, down=True)
    ft.paste('C2', item_kind)
    ft.paste('C5', item_code)
    ft.paste('C6', item_name_k)

    if fs:
        item_name = [item_name] if isinstance(item_name, str) else item_name
        item_freq = {1: '1st-Quarter', 2: 'Semi-Annual', 3: '3rd-Quarter', 4: 'Annual'}
        year, quarter = fs
        ft.paste('C3', [item_freq[quarter]]*len(item_name))
        ft.paste('C4', [year]*len(item_name))
        ft.refresh()
        df = ft.to_df('A6').drop('Name', axis=1).set_axis(['sym_cd']+item_name, axis=1)
        df.insert(1, 'year', year)
        df.insert(2, 'quarter', quarter)

    else:
        ft.paste('C3', item_freq)
        ft.paste('C4', [daily] * len(item_name))
        ft.refresh()
        df = ft.to_df('A6').drop('Name', axis=1).set_axis(['sym_cd']+item_name, axis=1)
        df.insert(0, 'base_dt', daily)

    df['sym_cd'] = df.sym_cd.str[1:]
    ft.close()
    col = ut.get_table_info(table, numeric=True, new=new)
    df[col] = df[col].apply(pd.to_numeric, errors='coerce')

    return df


def get_fiscal_basis_data(symbols, year_range, item=None, new=False):
    symbols = 'A' + pd.Series(symbols)

    if item:
        item_name, item_kind, item_code, item_name_k, item_freq = ut.get_table_info(item=item, new=new)
    else:
        item_name, item_kind, item_code, item_name_k, item_freq = ut.get_table_info('fs', new=new)

    ft = FnTools('fb_data.xlsm')
    ft.paste('A12', symbols, down=True)
    ft.paste('F8', item_kind)
    ft.paste('F9', item_code)
    ft.paste('F10', item_name_k)
    ft.paste('B7', year_range)
    ft.refresh()

    item_name = ['year', 'quarter', item_name] if isinstance(item_name, str) else ['year', 'quarter'] + item_name
    ft.paste('D11', item_name)
    df = ft.to_df('A11')

    freq = {'1st-Quarter': 1, 'Semi-Quarter': 2, '3rd-Quarter': 3, 'Annual': 4}
    df['quarter']=df.quarter.replace(freq)
    df.insert(0, 'sym_cd', df.Symbol.str[1:])
    df = df.drop(columns=['Symbol', 'Name', '결산월'])
    df[item_name] = df[item_name].apply(pd.to_numeric, errors='coerce')
    ft.close()

    return df.reset_index(drop=True)


def get_time_series_data(date_range, symbols, new=False):

    ft = FnTools('ts_data.xlsm')

    symbols = 'A' + pd.Series(symbols)
    sym_len = len(symbols)

    item_name, item_kind, item_code, item_name_k, item_freq = ut.get_table_info('daily', new=new)
    item_len = len(item_name)

    symbols_list = []
    for sym in symbols:
        symbols_list = symbols_list + [sym]*item_len

    items = pd.DataFrame(list(map(lambda x:x*sym_len, [item_kind, item_code, item_name_k, item_freq])))

    ft.paste('B8', date_range)
    ft.paste('B9', symbols_list)
    ft.paste('B11', items)
    ft.refresh()
    data = ft.to_df('A9').rename(columns={'Symbol': 'base_dt'}).loc[5:].set_index('base_dt')
    data.columns=pd.MultiIndex.from_arrays([data.columns.str[1:], item_name*sym_len])
    df = data.stack(0)[item_name].reset_index().rename(columns={'level_1': 'sym_cd'})

    col = ut.get_table_info('daily', numeric=True, new=new)
    df[col] = df[col].apply(pd.to_numeric, errors='coerce')

    return df.sort_values('base_dt', ignore_index=True)