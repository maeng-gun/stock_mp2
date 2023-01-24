= = = import pandas as pd
import input.raw_table as rt
from util import raw


def update_raw_data():
    daily_update = raw.select(table='daily_update')
    if daily_update is None:
        print("최근 영업일까지 업데이트 완료!")
        pass
    else:
        daily_update = pd.Series(daily_update)
        quarterly_update = raw.select(table='quarterly_update')
        stock_list = raw.select(table='stock_list')  # 업데이트 전 종목 목록

        # 영업일 업데이트
        for year in daily_update.str[:4].unique():
            rt.update_workdays_table(year)

        # 종목 일별지표 업데이터
        for date in daily_update:
            rt.get_stock_tables(date)

        # 업데이트된 종목 목록을 기존/신규로 분리
        daily_update_tuple = f'({daily_update.iloc[0]})' if len(daily_update) == 1 else tuple(daily_update)
        stock_list_updated = raw.select(f'select distinct sym_cd from stock_daily where base_dt in {daily_update_tuple}')
        old_symbols = stock_list_updated[stock_list_updated.isin(stock_list)]
        new_symbols = stock_list_updated[~stock_list_updated.isin(stock_list)]

        # 기존종목 최근분기 기업재무제표 업데이트
        for year, quarter in quarterly_update.iloc:
            rt.get_company_fs_tables(old_symbols, old=[year, quarter])
            # rt.get_company_fs_pl_prep_table(old=[year, quarter])

        # 신규종목 전 기간 기업재무제표 업데이트
        if not new_symbols.empty:
            start = raw.select(
                'select fs_q, quarter from company_fs_bs order by fs_q, quarter limit 1'
            ).iloc[0].to_list()
            end = raw.select(
                'select fs_q, quarter from company_fs_bs order by fs_q desc, quarter desc limit 1'
            ).iloc[0].to_list()
            rt.get_company_fs_tables(new_symbols, new=[start, end])
            # rt.get_company_fs_pl_prep_table(new=[start, end], new_sym=new_symbols)
        else:
            print("신규종목 없음")
