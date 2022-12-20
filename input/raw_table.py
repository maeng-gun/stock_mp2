import pandas as pd
import input.krx as krx
from util import raw, get_table_info
import input.fn as fn


def update_workdays_table(year, save=True):

    print(f'{str(year)}년 KRX 영업일 정보 스크래핑', end='...')
    holidays = krx.get_holidays_from_krx(year)
    freq_cbd = pd.offsets.CustomBusinessDay(holidays=holidays)
    workdays = pd.date_range(
        start=str(year)+'-01-01',
        end=str(year)+'-12-31',
        freq=freq_cbd
    ).to_frame(False, 'base_dt')

    workdays['fs_q_y'] = (workdays.base_dt - pd.DateOffset(months=5)).dt.year
    workdays['fs_q_q'] = (workdays.base_dt - pd.DateOffset(months=5)).dt.quarter
    workdays['fs_q_q'] = workdays['fs_q_q'].mask(workdays.base_dt.dt.month == 3, 3)
    workdays['fs_y'] = (workdays.base_dt - pd.DateOffset(months=15)).dt.year

    if save:
        raw.delete('workdays', f'year(base_dt)={year}')
        raw.insert('workdays', workdays)
        print('완료!')
    else:
        return workdays


def get_stock_tables(date, save=True):
    print(date, "종목 스크래핑 시작", end='...')
    df1 = krx.get_krx_stock_daily(date)
    df2 = fn.get_cross_section_data(df1.sym_cd, daily=date)
    df2['sym_mng'] = (df2.sym_mng.apply(type) == str) & (df2.sym_mng != '정상')
    df2['sym_stop'] = (df2.sym_stop.apply(type) == str) & (df2.sym_stop != '정상')
    df2['sym_reg'] = (df2.sym_reg.apply(type) == str) & (df2.sym_reg != '정상')
    pk1 = ['base_dt', 'sym_cd']
    pk2 = ['sym_cd', 'base_dt']
    info = ['sym_nm', 'mkt_cd', 'sec_krx']
    sec_fn = ['sec_fn_1', 'sec_fn_2', 'sec_fn_3']

    data1 = df1[df1.columns.drop(info)].merge(
        df2[df2.columns[:9]], how='left', on=pk1)
    data2 = df1[pk2+info].merge(df2[pk2+sec_fn]).rename(columns={'base_dt': 'info_update'})
    df3 = raw.select(table='stock_info', date_col='info_update')
    df3['info_update'] = df3.info_update.dt.strftime('%Y%m%d')
    data2 = pd.concat(
        [df3, data2]
    ).drop_duplicates(subset=data2.columns.drop('info_update'))

    data1['info_update'] = data1.apply(
        lambda x: data2[(data2.sym_cd == x.sym_cd) & (data2.info_update <= x.base_dt)].info_update.iloc[-1], axis=1
    )
    data2 = data2.query(f'info_update=="{date}"')
    data3 = df2[df2.columns[12:].insert(0, pk1)]
    data3 = data3.dropna(subset=data3.columns[2:], how='all')
    if save:
        raw.insert('stock_info', data2.copy())
        raw.insert('stock_daily', data1.copy())
        raw.insert('stock_daily_cons', data3.copy())
        print('완료!')
    else:
        print('완료!')
        return data1, data2, data3


def get_company_fs_tables(symbols, old=None, new=None, save=True):
    """
    old : 기존 종목인 경우. [최근연도, 최근분기]
    new : 신규 종목인 경우. [[시작연도, 시작분기], [최근연도, 최근분기]]"
    """
    col1 = get_table_info('fs', 'bs')
    col2 = get_table_info('fs', 'pl')

    if old:
        print(f"{old[0]}년 {old[1]}분기 기존종목 기업재무제표 스크래핑 시작", end='...')
        old_1 = raw.select(f"""
        select max(concat(year, quarter)) from fs_q where concat(year, quarter) < {"".join(map(str, old))}
        """)

        df1 = raw.select(f'select * from company_fs_bs where year = {old_1[:4]} and quarter = {old_1[4:]}')
        df2 = fn.get_cross_section_data(symbols, fs=old)

        df3 = pd.concat([df1, df2[['sym_cd', 'year', 'quarter'] + col1]])
        df3[col1] = df3.groupby('sym_cd')[col1].ffill()
        company_fs_bs = df3.query(f'year == {old[0]} and quarter == {old[1]}')

        company_fs_pl = df2[['sym_cd', 'year', 'quarter'] + col2].dropna(how='all', subset=col2)

    else:
        year_range = [new[0][0], new[1][0]]
        print(f"{year_range[0]}년 ~ {year_range[1]}년 신규종목 기업재무제표 스크래핑 시작", end='...')
        data = fn.get_fiscal_basis_data(symbols, year_range)

        last_yq = str(new[1][0])+str(new[1][1])
        data = data[data.year.astype('int').astype('str') + data.quarter.astype('str') <= last_yq]

        company_fs_bs = data[['sym_cd', 'year', 'quarter'] + col1].copy()
        company_fs_bs[col1] = company_fs_bs.groupby('sym_cd')[col1].ffill()

        company_fs_pl = data[['sym_cd', 'year', 'quarter'] + col2].dropna(how='all', subset=col2)

    if save:
        raw.upsert('company_fs_bs', company_fs_bs.copy())
        raw.upsert('company_fs_pl', company_fs_pl.copy())
        print('완료!')

    else:
        print('완료!')
        return company_fs_bs, company_fs_pl


def get_company_fs_pl_prep_table(old=None, new=None, new_sym=None, save=True):
    """
    old 또는 new 둘중 하나만 입력한다
    old : [최근연도, 최근분기]
    new : [[시작연도, 시작분기], [최근연도, 최근분기]]
    """
    if old:
        print(f"{old[0]}년 {old[1]}분기 기업재무제표 PL 지표 전처리 시작", end='...')
        start = str(old[0] - 1) + str(1)
        end = str(old[0]) + str(old[1])
    elif new:
        print(f"{new[0][0]}년 {new[0][1]}분기 ~ "
              f"{new[1][0]}년 {new[1][1]}분기 기업재무제표 PL 지표 전처리 시작", end='...')
        start = str(new[0][0]) + str(new[0][1])
        end = str(new[1][0]) + str(new[1][1])
    else:
        print("cur 또는 range를 입력하시오")
        return None

    query = f"""
    select a.sym_cd as sym_cd,
           a.year as year,
           a.quarter as quarter,
           b.sales as sales,
           b.opr_prof as opr_prof,
           b.earn as earn,
           b.earn_dom as earn_dom,
           b.op_cash_flow as op_cash_flow,
           b.cash_flow as cash_flow

    from company_fs_bs a left join company_fs_pl b
                                   on a.sym_cd = b.sym_cd and
                                      a.year = b.year and
                                      a.quarter = b.quarter
    where (concat(a.year, a.quarter) between {start} and {end})"""

    if new_sym is not None:
        new_sym = f'({new_sym.iloc[0]})' if len(new_sym) == 1 else tuple(new_sym)
        query = query + f'and a.sym_cd in {new_sym}'

    df1 = raw.select(query)
    col = df1.columns[3:]
    df = df1[['sym_cd', 'year', 'quarter']].copy()
    df2 = pd.concat([df1,
                     df1.groupby('sym_cd')[col].shift(4).add_suffix('_4q'),
                     df1.groupby(['sym_cd', 'year'])[col].shift(1, fill_value=0).add_suffix('_1q')
                     ], axis=1)
    df3 = df1.loc[df1.quarter == 4, col.insert(0, ['sym_cd', 'year'])].rename(columns=dict(zip(col, col+'_1y')))
    df3['year'] = df3.year + 1
    df4 = df2.merge(df3, how='left', on=['sym_cd', 'year'])
    df.loc[df.quarter == 4, col+'_a4q'] = df4.loc[df4.quarter == 4, col].values
    for i in col:
        df[i+'_a4q'] = df[i+'_a4q'].mask(df[i+'_a4q'].isna(), df4[i] + df4[i+'_1y'] - df4[i+'_4q'])
        df[i+'_a4q'] = df[i+'_a4q'].mask(df[i+'_a4q'].isna(), df4[i+'_1y'])
    df[col+'_n'] = df4[col].values - df4[col+'_1q'].values
    df['earn_a4q'] = df.earn_dom_a4q.mask(df.earn_dom_a4q.isna(), df.earn_a4q)
    df['earn_n'] = df.earn_dom_n.mask(df.earn_dom_n.isna(), df.earn_n)
    df = df.drop(columns=['earn_dom_a4q', 'earn_dom_n'])

    if old:
        result = df[(df.year == old[0]) & (df.quarter == old[1])].copy()
    else:
        result = df.copy()

    if save:
        raw.upsert('company_fs_pl_prep', result)
        print('완료!')
    else:
        print('완료!')
        return result
