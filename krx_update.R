
#[실행] DB 영업일 테이블(workdays) 갱신====
last_updated_date <- 
  db_obj('stock_daily') %>%
  summarise(base_dt=max(base_dt)) %>% pull()

y <- last_updated_date %>% year() %>% 
  c(., year(today())) %>% unique()

map_int(y, ~db_del(table = 'workdays',
                   condition = glue("year(base_dt) = {.x}")))
workdays <- map_dfr(y, get_workdays_from_krx)
db_upsert(table = 'workdays', df = workdays, col = 'base_dt')


#[함수] 일별 종목/인덱스/ETF 정보 얻기 =================

yyyymmdd <- '20221101'

print(glue('{yyyymmdd} 개별종목 일별지표 크롤링...'))

#개별종목 가격정보
params1 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01501",
                mktId = "ALL",
                trdDd = yyyymmdd)
df1 <- post_krx('data', params1)

params2 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01602",
                mktId = 'ALL',
                strtDd = yyyymmdd,
                endDd = yyyymmdd,
                adjStkPrc_check = 'Y')
df2 <- post_krx('data', params2) %>% 
  select(ISU_SRT_CD, BAS_PRC)

col_old <- c('ISU_SRT_CD', 'ACC_TRDVOL', 'ACC_TRDVAL', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'BAS_PRC', 'ISU_ABBRV', 'MKT_ID')
col_new <- c('sym_cd', 'trd_vol', 'trd_val', 'open', 'high', 'low', 'close', 'base_p', 'sym_nm', 'mkt_cd')

df3 <-
  left_join(df1,df2, by='ISU_SRT_CD')[col_old] %>%
  setNames(col_new) %>% 
  mutate(across(!c(sym_cd,sym_nm,mkt_cd), parse_number)) %>%
  filter(!is.na(sym_cd)) %>%
  mutate(ret = (close / base_p) - 1,
         base_dt = ymd(yyyymmdd),
         base_p = ifelse(base_p==1, NA, base_p)) %>%
  relocate(base_dt, .before = 1) %>% 
  filter( mkt_cd!='KNX',
          str_detect(sym_cd, '0$'),
         !str_detect(sym_cd, '^9'),
         !str_detect(sym_nm, '스팩'),
         !is.na(base_p))

#인프라펀드/선박투자/인프라투자 종목 정보
blds = list(p12014 = "dbms/MDC/STAT/standard/MDCSTAT02901",
            p12015 = "dbms/MDC/STAT/standard/MDCSTAT02801",
            p12016 = "dbms/MDC/STAT/standard/MDCSTAT03001")
fcn_inv <- function(bld){
  params <- list(bld = blds[bld],
                 trdDd = yyyymmdd)
  data <- post_krx('data', params) %>% 
    transmute(sym_cd = ISU_SRT_CD)
  return(data)}
df4 <- map_dfr(names(blds), fcn_inv)

#KRX 섹터정보
sector_match = c('^음식료·담배$' = '음식료품', 
                 '^섬유·의류$' = '섬유의복', 
                 '^종이·목재$' = '종이목재',
                 '^출판·매체복제$' = '종이목재', 
                 '^제약$' = '의약품', 
                 '^비금속$' = '비금속광물', 
                 '^금속$' = '철강금속',
                 '^기계·장비$' = '기계', 
                 '^일반전기전자$' = '전기전자', 
                 '^의료·정밀기기$' = '의료정밀',
                 '^운송장비·부품$' = '운수장비', 
                 '^기타제조$' = '기타제조업', 
                 '^전기·가스·수도$' = '전기가스업',
                 '^건설$' = '건설업', 
                 '^유통$' = '유통업', 
                 '^숙박·음식$' = '서비스업', 
                 '^운송$' = '운수창고업',
                 '^금융$' = '기타금융', 
                 '^기타서비스$' = '서비스업', 
                 '^오락·문화$' = '서비스업', 
                 '^통신서비스$' = '통신업',
                 '^방송서비스$' = '서비스업', 
                 '^인터넷$' = '서비스업', 
                 '^디지털컨텐츠$' = '서비스업', 
                 '^소프트웨어$' = '서비스업',
                 '^컴퓨터서비스$' = '서비스업', 
                 '^통신장비$' = '전기전자', 
                 '^정보기기$' = '전기전자', 
                 '^반도체$' = '전기전자',
                 '^IT부품$' = '전기전자')

fcn_sec = function(mkt){
  params = list(bld = "dbms/MDC/STAT/standard/MDCSTAT03901",
                mktId = mkt,
                trdDd = yyyymmdd)
  data = post_krx("data", params)
  if(nrow(data) > 0){
    col = c("ISU_SRT_CD", "IDX_IND_NM")
    col_new = c("sym_cd", "sec_krx")
    data = data[col] %>% 
      set_names(col_new) %>% 
      mutate(sec_krx = str_replace_all(sec_krx,sector_match))
  }
  return(data)
}

df5 <- map_dfr(c('STK','KSQ'), fcn_sec)

#krx-data 기초자료
krx_data <- df3  %>% 
  anti_join(df4, by='sym_cd') %>% 
  left_join(df5, by='sym_cd') %>% 
  arrange(sym_cd)

#fn가이드 기초자료
fn_data <- 
  fn$get_cross_section_data(krx_data$sym_cd, 
                            daily=yyyymmdd) %>% 
  tibble() %>% 
  mutate(base_dt = ymd(base_dt))

#stock_info 테이블 업데이트====
stock_info <- db_obj('stock_info') %>% collect() %>% 
  bind_rows(
    krx_data %>% 
      select(sym_cd, base_dt, sym_nm, 
             mkt_cd, sec_krx) %>% 
      left_join(
        fn_data %>% select(sym_cd, base_dt, sec_fn_1, 
                       sec_fn_2, sec_fn_3),
        by = c('base_dt','sym_cd')) %>% 
      rename('info_update'='base_dt')) %>% 
  distinct(across(-info_update), .keep_all = T)

# db_upsert('stock_info',
#           stock_info %>%
#             filter(info_update==ymd(yyyymmdd)),
#           c('sym_cd','info_update'))

#stock_daily 테이블 업데이트====
max_key <- db_obj('stock_daily') %>% 
  summarise(a=max(num_key)) %>% pull()

stock_daily <- krx_data %>% 
  select(base_dt:base_p, ret) %>% 
  left_join(
    fn_data %>% select(base_dt,sym_cd,
                   net_buy_fi:num_stock_t),
    by=c('base_dt','sym_cd')) %>% 
  left_join(
    stock_info %>%
      select(sym_cd, info_update) %>% 
      group_by(sym_cd) %>% 
      slice(n()),
    by='sym_cd') %>% 
  mutate(num_key = max_key + 1:n(),
         .before=1)

<<<<<<< HEAD
# db_upsert('stock_daily', stock_daily,
#           c('base_dt','sym_cd'))
=======
db_obj('stock_daily') %>% 
  summarise(max(num_key))

names(df9)



>>>>>>> 03bb284594dedea1c31359b15bddf0ce71036194

#stock_cons 테이블 업데이트====
stock_cons <- stock_daily %>% 
  select(num_key:sym_cd) %>% 
  right_join(
    fn_data %>% 
      select(base_dt, sym_cd, 
             sales_12mf:eps_down_fy2) %>% 
      filter(!if_all(sales_12mf:eps_down_fy2, is.na)),
    by=c('base_dt','sym_cd')) %>% 
  select(!(base_dt:sym_cd))

# db_upsert('stock_cons', stock_cons, 
#           c('num_key'))

#stock_managed 테이블  테이블====
stock_managed <- stock_daily %>% 
  select(num_key:sym_cd) %>% 
  right_join(fn_data %>% select(base_dt:sym_reg),
             by = c("base_dt", "sym_cd")) %>% 
  pivot_longer(sym_mng:sym_reg, 
               names_to = 'managed',
               values_to = 'value') %>% 
  filter(value!='정상') %>% 
  transmute(
    num_key = num_key,
    type = as.integer(str_replace_all(
      managed, c('sym_mng' = '1', 
                 'sym_stop'='2', 
                 'sym_reg'='3'))))

# db_upsert('stock_managed', stock_managed, 
#           c('num_key'))

#인덱스 구성종목 정보
idx_list <- 
  post_krx('data', 
           list(bld = "dbms/comm/finder/finder_equidx",
                mktsel = '1')) %>% 
  mutate(mkt_nm = str_replace_all(marketName,
                                  c("KRX"="X", "KOSPI"="P",
                                    "KOSDAQ"="Q", "테마"="T")), 
         .before = short_code) %>% 
  unite('idx_code', mkt_nm:short_code,
        sep='', remove=F) %>% 
  select(-marketCode,-marketName)

print(paste(yyyymmdd, "인덱스 구성종목 크롤링..."))

params_list <- idx_list %>% 
  transmute(
    indIdx = full_code, 
    indIdx2 = short_code, 
    tboxindIdx_finder_equidx0_2 = codeName, 
    codeNmindIdx_finder_equidx0_2 = codeName,
    idx_code = idx_code,
    bld = "dbms/MDC/STAT/standard/MDCSTAT00601",
    trdDd = yyyymmdd) %>%
  apply(1,as.list)

get_stocks_in_idx <- function(params){
  df = post_krx("data",params)
  Sys.sleep(0.05)
  if(nrow(df)!=0){
    df %>% 
      transmute(base_dt = ymd(yyyymmdd),
                sym_cd = ISU_SRT_CD,
                idx_code = params$idx_code)
  } else df 
}

#stocks_in_index 테이블 업데이트====
stocks_in_index <- 
  map_dfr(params_list, get_stocks_in_idx) %>% 
  inner_join(
    stock_daily %>% select(num_key:sym_cd), 
    by=c('base_dt','sym_cd')) %>% 
  select(num_key, idx_code) %>% 
  arrange(num_key, idx_code)

# db_upsert('stocks_in_index', stocks_in_index,
#           cols=c('num_key','idx_code'))


# 인덱스 가격 정보
mkt_nm_list <- c("01" = "X", "02" = "P", "03" = "Q", "04" = "T")

get_index_daily <- function(mkt){
  params1 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT00101",
                  idxIndMidclssCd = mkt,
                  trdDd = yyyymmdd,
                  share = "1",
                  money = "1")
  params2 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT00201",
                  idxIndMidclssCd = mkt,
                  strtDd = yyyymmdd,
                  endDd = yyyymmdd)
  df1 <- post_krx("data", params1)
  df2 <- post_krx("data", params2)
  if (nrow(df1)!=0) {
    col1 <- c("IDX_NM", "ACC_TRDVOL", "ACC_TRDVAL", "OPNPRC_IDX", 
              "HGPRC_IDX", "LWPRC_IDX", "CLSPRC_IDX")
    col_new1 <- c("sym_nm", "trd_vol", "trd_val", "open", 
                  "high", "low", "close")
    col2 <- c("IDX_IND_NM", "OPN_DD_INDX")
    col_new2 <- c("sym_nm", "base_p")
    
    df <- df1[col1] %>% set_names(col_new1) %>% 
      left_join(df2[col2] %>% set_names(col_new2),
                by="sym_nm") %>% 
      filter(if_all(-sym_nm, ~.!="-")) %>% 
      mutate(across(!sym_nm, parse_number),
             base_dt = yyyymmdd) %>% 
      left_join(idx_list %>% 
                  filter(mkt_nm==mkt_nm_list[mkt]) %>% 
                  select(idx_code,codeName),
                by = c("sym_nm" = "codeName")) %>% 
      relocate(c(base_dt,idx_code)) %>% 
      select(-sym_nm)
    
    return(df)
  } else df1
}

# index_daily 테이블 업데이트====
index_daily <- 
  map_dfr(names(mkt_nm_list), get_index_daily) %>% 
  arrange(idx_code)

# ETF 가격 정보

# etf_daily 테이블 업데이트====




#[실행] DB 일별 테이블(daily) 업데이트=======
yyyymmdd <- '20221028'

stock_list <- 
  db_obj('stock_info') %>% 
  distinct(sym_cd)

last_workday <- today() - 
  (ifelse(hour(now()) < 9, 2, 1) %>% days())

daily_update <- 
  db_obj('workdays') %>% 
  filter(base_dt > last_updated_date,
         base_dt <= last_workday) %>% 
  pull(base_dt) %>% strftime('%Y%m%d')

