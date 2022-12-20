import requests as req
from numpy import nan, inf
import pandas as pd
import functools
import time


def krx_req_post(site, params):
    """
    krx 사이트에서 POST 요청을 통해 얻은 JSON 파일을 데이터프레임으로 반환

    Parameters
    ----------
    site : str
        'data' - data.krx.co.kr 사이트 인 경우
        'open' - open.krx.co.kr 사이트인 경우

    params : dict
        POST 요청 시 세부정보를 담은 딕셔너리

    Returns
    -------
    pd.DataFrame (데이터프레임 형식)
    """

    url = {'data': 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
           'open': 'http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx'}
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 ",
               'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'}
    res = req.post(url[site], data=params, headers=headers).json()
    key = list(res.keys())[0]
    data = pd.DataFrame(res[key])
    return data


def krx_req_get(otp_params):
    """
    KRX 사이트(open.krx.co.kr)에서 GET요청을 보내 OTP 코드를 얻는 함수

    Parameters
    ----------
    otp_params : dict
        GET 요청시 쿼리스트링 파라미터들을 담은 딕셔너리

    Returns
    -------
    str (문자열 형식)

    """
    url = 'http://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 "}
    otp_code = req.get(url, params=otp_params, headers=headers).text
    return otp_code


def get_holidays_from_krx(year):
    unix_time = str(int(round(time.time() * 1000)))
    otp_params = {'bld': 'MKD/01/0110/01100305/mkd01100305_01',
                  'name': 'form',
                  '_': unix_time}
    otp_code = krx_req_get(otp_params)

    # 휴장일 표 얻기
    view_params = {'search_bas_yy': str(year),
                   'gridTp': 'KRX',
                   'pagePath': 'contents/MKD/01/0110/01100305/MKD01100305.jsp',
                   'code': otp_code}
    holidays = krx_req_post('open', view_params)['calnd_dd']
    return holidays


def get_krx_stock_list_in_investment_company(date):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 기타증권 - [12014],[12015],[12016]
    """
    bld = {"12014": "dbms/MDC/STAT/standard/MDCSTAT02901",
           "12015": "dbms/MDC/STAT/standard/MDCSTAT02801",
           "12016": "dbms/MDC/STAT/standard/MDCSTAT03001"}

    def read(num):
        params = {'bld': bld[num],
                  'trdDd': date}
        data = krx_req_post('data', params)
        col = ['ISU_SRT_CD']
        col_new = ['sym_cd']
        data = data[col].set_axis(col_new, axis=1)
        data['inv_com'] = True
        return data

    df1 = read('12014')
    df2 = read('12015')
    df3 = read('12016')
    result = pd.concat([df1, df2, df3]).reset_index(drop=True)
    return result.fillna(False)


def get_krx_stock_list_in_index(date, index_list_k, index_list):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 세부안내 - [11006]지수구성종목
    """
    col = ['indIdx', 'indIdx2', 'tboxindIdx_finder_equidx0_2', 'codeNmindIdx_finder_equidx0_2']
    idx_list = krx_req_post('data', {'bld': "dbms/comm/finder/finder_equidx", 'mktsel': '1'})
    idx = idx_list[['full_code', 'short_code', 'codeName', 'codeName']].set_axis(col, axis=1)

    df_list = []
    col = ['ISU_SRT_CD']
    col_new = ['sym_cd']

    for index in index_list_k:
        try:
            params = idx[idx.tboxindIdx_finder_equidx0_2 == index].iloc[0].to_dict()
            params.update(bld="dbms/MDC/STAT/standard/MDCSTAT00601",
                          trdDd=date)
            df = krx_req_post('data', params)[col].set_axis(col_new, axis=1)
            df[index.replace(' ', '')] = True
            df_list.append(df)
        except Exception:
            cols = col_new + [index.replace(' ', '')]
            df = pd.DataFrame(columns=cols)
            df_list.append(df)

    data = functools.reduce(lambda left, right: pd.merge(left, right, how='outer', on=col_new), df_list)
    data.columns = col_new + index_list
    return data.fillna(False)


def get_krx_stock_sector(date):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 세부안내 - [12025]업종분류현황
    """
    sector_match = {'음식료·담배': '음식료품', '섬유·의류': '섬유의복', '종이·목재': '종이목재',
                    '출판·매체복제': '종이목재', '제약': '의약품', '비금속': '비금속광물', '금속': '철강금속',
                    '기계·장비': '기계', '일반전기전자': '전기전자', '의료·정밀기기': '의료정밀',
                    '운송장비·부품': '운수장비', '기타제조': '기타제조업', '전기·가스·수도': '전기가스업',
                    '건설': '건설업', '유통': '유통업', '숙박·음식': '서비스업', '운송': '운수창고업',
                    '금융': '기타금융', '기타서비스': '서비스업', '오락·문화': '서비스업', '통신서비스': '통신업',
                    '방송서비스': '서비스업', '인터넷': '서비스업', '디지털컨텐츠': '서비스업', '소프트웨어': '서비스업',
                    '컴퓨터서비스': '서비스업', '통신장비': '전기전자', '정보기기': '전기전자', '반도체': '전기전자',
                    'IT부품': '전기전자'}

    def read(mkt):
        params = {'bld': "dbms/MDC/STAT/standard/MDCSTAT03901",
                  'mktId': mkt,
                  'trdDd': date}
        data = krx_req_post('data', params)
        if not data.empty:
            col = ['ISU_SRT_CD', 'IDX_IND_NM']
            col_new = ['sym_cd', 'sec_krx']
            data = data[col].set_axis(col_new, axis=1)
            data['sec_krx'] = data.sec_krx.replace(sector_match)
        return data

    df1 = read('STK')
    df2 = read('KSQ')
    return pd.concat([df1, df2]).reset_index(drop=True)


def get_krx_stock_daily(date):

    #KRX정보시스템 전종목시세[12001] 페이지 정보
    params1 = {'bld': "dbms/MDC/STAT/standard/MDCSTAT01501",
               'mktId': 'ALL',
               'trdDd': date}
    data1 = krx_req_post('data', params1)

    #KRX정보시스템 전종목등락률[12002] 페이지 정보
    params2 = {'bld': "dbms/MDC/STAT/standard/MDCSTAT01602",
               'mktId': 'ALL',
               'strtDd': date,
               'endDd': date,
               'adjStkPrc_check': 'Y'}
    data2 = krx_req_post('data', params2)[['ISU_SRT_CD', 'BAS_PRC']].copy()

    if not data1.empty:
        #병합
        df1 = data1.merge(data2, 'left', 'ISU_SRT_CD')
        col = ['ISU_SRT_CD', 'ACC_TRDVOL', 'ACC_TRDVAL', 'TDD_OPNPRC', 'TDD_HGPRC',
               'TDD_LWPRC', 'TDD_CLSPRC', 'BAS_PRC', 'ISU_ABBRV', 'MKT_ID']
        col_new = ['sym_cd', 'trd_vol', 'trd_val', 'open', 'high', 'low', 'close', 'base_p', 'sym_nm', 'mkt_cd']
        df1 = df1[col].set_axis(col_new, axis=1).copy()

        #숫자처리
        df1[df1.columns[1:8]] = df1[df1.columns[1:8]].replace('-', nan) \
            .replace(',', '', regex=True).copy()
        df1[df1.columns[1:8]] = df1[df1.columns[1:8]].apply(pd.to_numeric) \
            .astype('float').copy()

        #투자회사 여부 확인
        df2 = get_krx_stock_list_in_investment_company(date)

        #대표인덱스 포함여부 확인
        index_list_k = ['코스피 200', '코스닥 150', '코스닥 우량기업부']
        index_list = ['ksp_200', 'ksq_150', 'ksq_bc']
        df3 = get_krx_stock_list_in_index(date, index_list_k, index_list)

        #krx산업분류 확인
        df4 = get_krx_stock_sector(date)

        #병합
        df_list = [df1, df2, df3, df4]
        data = functools.reduce(lambda left, right:
                                pd.merge(left, right, how='left', on='sym_cd'), df_list)
        data[['inv_com'] + index_list] = data[['inv_com'] + index_list].fillna(False)
        data['sec_krx'] = data.sec_krx.fillna('미분류')

        #불필요한 종목 제거
        data['base_p'] = data.base_p.replace(1, None)
        data = data[(data.sym_cd.str[-1] == '0') & (data.sym_cd.str[0] != '9')
                    & (~data.sym_nm.str.contains('스팩')) & (data.mkt_cd != 'KNX')
                    & (~data.inv_com)].dropna(subset=['base_p']).drop(columns='inv_com').copy()

        #날짜 추가
        data.insert(0, 'base_dt', date)
        return data

    else:
        return pd.DataFrame()


def get_krx_etf_prices(date):
    params1 = {'bld': "dbms/MDC/STAT/standard/MDCSTAT04301",
               'trdDd': date}
    params2 = {'bld': "dbms/MDC/STAT/standard/MDCSTAT04401",
               'strtDd': date,
               'endDd': date}
    data1 = krx_req_post('data', params1)
    data2 = krx_req_post('data', params2)
    if not data1.empty:
        col1 = ['ISU_SRT_CD', 'ISU_ABBRV', 'ACC_TRDVOL', 'ACC_TRDVAL',
                'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC']
        col_new1 = ['sym_cd', 'sym_nm', 'trd_vol', 'trd_val',
                    'open', 'high', 'low', 'close']
        data1 = data1[col1].set_axis(col_new1, axis=1).copy()
        col2 = ['ISU_SRT_CD', 'BAS_PRC']
        col_new2 = ['sym_cd', 'base_p']
        data2 = data2[col2].set_axis(col_new2, axis=1).copy()
        data = data1.merge(data2, 'left', 'sym_cd')
        data.insert(2, 'mkt_cd', 'ETF')
        data[data.columns[3:]] = data[data.columns[3:]].replace('-', nan) \
            .replace(',', '', regex=True).copy()
        data = data.dropna(subset=['base_p'])
        data[data.columns[3:]] = data[data.columns[3:]].apply(pd.to_numeric) \
            .astype('float').copy()
        data['base_p']=data.base_p.replace(1, 0)
        data['ret'] = ((data.close / data.base_p) - 1).replace(inf, nan)
        data.insert(0, 'base_dt', date)

    return data


def get_krx_index_prices(date):

    data1 = pd.DataFrame()
    mkt_cd_list = {'01': 'KRX', '02': 'STK', '03': 'KSQ', '04': 'THM'}
    for mkt in ['01', '02', '03', '04']:
        params1 = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT00101',
                   'idxIndMidclssCd': mkt,
                   'trdDd': date,
                   'share': '1',
                   'money': '1'}
        params2 = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT00201',
                   'idxIndMidclssCd': mkt,
                   'strtDd': date,
                   'endDd': date}
        df1 = krx_req_post('data', params1)
        df2 = krx_req_post('data', params2)
        if not df1.empty:
            col1 = ['IDX_NM', 'ACC_TRDVOL', 'ACC_TRDVAL', 'OPNPRC_IDX', 'HGPRC_IDX', 'LWPRC_IDX', 'CLSPRC_IDX']
            col_new1 = ['sym_nm', 'trd_vol', 'trd_val', 'open', 'high', 'low', 'close']
            df1 = df1[col1].set_axis(col_new1, axis=1).copy()
            col2 = ['IDX_IND_NM', 'OPN_DD_INDX']
            col_new2 = ['sym_nm', 'base_p']
            df2 = df2[col2].set_axis(col_new2, axis=1).copy()
            df = df1.merge(df2, 'left', 'sym_nm')
            df.insert(2, 'mkt_cd', mkt_cd_list[mkt])
            data1 = pd.concat([data1, df])

    params2 = {'locale': 'ko_KR',
               'bld': 'dbms/comm/finder/finder_equidx',
               'mktsel': '1'}
    data2 = krx_req_post('data', params2).rename(columns={'codeName': 'sym_nm',
                                                          'marketName': 'mkt_cd'})
    data2['sym_cd'] = data2.full_code + '.' + data2.short_code
    data2 = data2[['sym_cd', 'sym_nm', 'mkt_cd']].copy()
    data = data2.merge(data1, how='right', on=['sym_nm', 'mkt_cd'])
    data[data.columns[3:]] = data[data.columns[3:]].replace('-', nan) \
        .replace(',', '', regex=True).copy()
    data = data.dropna(subset=['base_p'])
    data[data.columns[3:]] = data[data.columns[3:]].apply(pd.to_numeric) \
        .astype('float').copy()
    data['base_p']=data.base_p.replace(1, 0)
    data.insert(0, 'base_dt', date)


    return data






def get_krx_stock_info(date):

    index_list_k = ['코스피 200', '코스닥 150', '코스닥 우량기업부']
    index_list = ['ksp_200', 'ksq_150', 'ksq_bc']

    df1 = get_krx_stock_daily(date)
    df2 = get_krx_stock_sector(date)
    df3 = get_krx_stock_list_in_investment_company(date)
    df4 = get_krx_stock_list_in_index(date, index_list_k, index_list)

    df_list = [df1, df2, df3, df4]
    data = functools.reduce(lambda left, right:
                            pd.merge(left, right, how='left', on='sym_cd'), df_list)
    data[['inv_com'] + index_list] = data[['inv_com'] + index_list].fillna(False)
    data['sec_krx'] = data.sec_krx.fillna('미분류')
    data = data[(data.sym_cd.str[-1] == '0') & (data.sym_cd.str[0] != '9') & (~data.inv_com) & (~data.sym_nm.str.contains('스팩'))]
    return data


