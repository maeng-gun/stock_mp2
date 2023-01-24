source("utils.R")

#[변수] 업데이트 대상 기간 얻기====
all_stocks_list <- 
  db_obj('stock_info') %>% 
  distinct(sym_cd) %>% 
  collect()

last_updated_date <- 
  db_obj('stock_daily') %>%
  summarise(base_dt=max(base_dt, na.rm=T)) %>% 
  pull()

last_workday <- today() - 
  (ifelse(hour(now()) < 9, 2, 1) %>% days())

last_update_quarter <- 
  db_obj('company_fs_bs') %>%
  summarise(q=max(fs_q)) %>% pull(q) %>% 
  as.yearqtr()

update_years <- last_updated_date %>% year() %>% 
  c(., year(today())) %>% unique()

update_days <- 
  db_obj('workdays') %>% 
  filter(base_dt > last_updated_date,
         base_dt <= last_workday) %>% 
  collect()

update_quarters <- 
  update_days %>% 
  mutate(fs_q = as.yearqtr(fs_q)) %>% 
  filter(fs_q >= last_update_quarter) %>% 
  distinct(fs_q)

update_days <- 
  update_days %>% pull(base_dt) %>% 
  strftime('%Y%m%d')
  
#[함수] 연도별 영업일 정보 얻기====
get_workdays_from_krx <- function(year, save=F){
  
  print(glue('{year}년 KRX 영업일 정보 크롤링...'))
  
  url <- 'http://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
  unix_time <- 
    (as.numeric(Sys.time()) * 1000) %>% 
    round() %>% as.character()
  otp_params <- list(
    bld = 'MKD/01/0110/01100305/mkd01100305_01',
    name = 'form',
    '_' = unix_time)
  user.agent <- 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 '
  
  otp_code <- 
    GET(url=url,
        query=otp_params,
        user_agent(user.agent)) %>% 
    content('t')
  
  view_params <- list(
    search_bas_yy = as.character(year),
    gridTp = 'KRX',
    pagePath = 'contents/MKD/01/0110/01100305/MKD01100305
.jsp',
    code = otp_code)
  
  holidays <- 
    post_krx('open',view_params)$calnd_dd %>% 
    as.Date()
  
  start <- glue('{year}-01-01')
  end <- glue('{year}-12-31')

  cal <- create.calendar(
    name = 'mycal',
    holidays = holidays,
    weekdays = c('saturday','sunday'),
    start.date = start,
    end.date = end)
  
  workdays <- bizseq(start, end, cal)
  i <- ifelse(month(workdays)==3, 6, 5)
  
  res <- 
    tibble(base_dt = workdays,
           fs_q = (workdays - months(i)) %>% 
             as.yearqtr() %>% 
             as.character(),
           fs_y = (workdays-months(15)) %>% 
             year()) %>% 
    fill(everything())
  
  if(save){
    db_del(table = 'workdays',
           query = glue("year(base_dt) = {year}"))
    
    db_upsert(table = 'workdays', 
              df = res, 
              col = 'base_dt')
  }
  return(res)
}


#[실행] workdays 테이블 업데이트====
workdays <- map_dfr(update_year, 
                    ~get_workdays_from_krx(.x, save=T))


#[실행] index_list 테이블 업데이트====
index_list <- 
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

db_upsert('index_list', index_list, cols='idx_code')

#[함수] 일별 종목 정보 얻기 =================
get_daily_tables <- function(yyyymmdd, save=F){
  print(glue('{yyyymmdd} 개별종목 일별지표 크롤링...'))
  print('- KRX 정보시스템 크롤링...')
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
    mutate(base_dt = ymd(yyyymmdd),
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
  print('- DataGuide 크롤링...')
  fn_data <- 
    fn$get_cross_section_data(krx_data$sym_cd, 
                              daily=yyyymmdd) %>% 
    tibble() %>%
    mutate(base_dt = ymd(base_dt),
           across(everything(), ~replace_na(.x, NA)))
  
  #stock_info 테이블 생성====
  print('- stock_info 테이블 생성...')
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

  #stock_daily 테이블 생성====
  print('- stock_daily 테이블 생성...')
  max_key <- db_obj('stock_daily') %>% 
    summarise(a=max(num_key)) %>% pull()
  
  stock_daily <- krx_data %>% 
    select(base_dt:base_p) %>% 
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

  stock_info <- 
    stock_info %>% filter(info_update==ymd(yyyymmdd))
  
  #stock_cons 테이블 생성====
  print('- stock_cons 테이블 생성...')
  stock_cons <- stock_daily %>% 
    select(num_key:sym_cd) %>% 
    right_join(
      fn_data %>% 
        select(base_dt, sym_cd, 
               sales_12mf:eps_down_fy2) %>% 
        filter(!if_all(sales_12mf:eps_down_fy2, is.na)),
      by=c('base_dt','sym_cd')) %>% 
    select(!(base_dt:sym_cd))

  #stock_managed 테이블 생성====
  print('- stock_managed 테이블 생성...')
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
  
  # stocks_in_index 테이블 생성====
  print("- stocks_in_index 테이블 생성")
  index_list <- 
    db_obj('index_list') %>% collect()
  
  params_list <- index_list %>% 
    transmute(
      indIdx = full_code, 
      indIdx2 = short_code, 
      tboxindIdx_finder_equidx0_2 = codeName, 
      codeNmindIdx_finder_equidx0_2 = codeName,
      idx_code = idx_code,
      bld = "dbms/MDC/STAT/standard/MDCSTAT00601",
      trdDd = yyyymmdd) %>%
    apply(1,as.list)
  
  get_stocks_in_index <- function(params){
    df = post_krx("data",params)
    Sys.sleep(0.05)
    if(nrow(df)!=0){
      df %>% 
        transmute(base_dt = ymd(yyyymmdd),
                  sym_cd = ISU_SRT_CD,
                  idx_code = params$idx_code)
    } else df 
  }
  
  stocks_in_index <- 
    stock_daily %>% select(num_key:sym_cd)%>% 
    inner_join(
      map_dfr(params_list, get_stocks_in_index), 
      by=c('base_dt','sym_cd')) %>% 
    select(num_key, idx_code) %>% 
    arrange(num_key, idx_code)
  
  # index_daily 테이블 생성====
  print("- index_daily 테이블 생성")
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
        left_join(index_list %>% 
                    filter(mkt_nm==mkt_nm_list[mkt]) %>% 
                    select(idx_code,codeName),
                  by = c("sym_nm" = "codeName")) %>% 
        relocate(c(base_dt,idx_code)) %>% 
        select(-sym_nm)
      
      return(df)
    } else df1
  }
  
  index_daily <- 
    map_dfr(names(mkt_nm_list), get_index_daily) %>% 
    arrange(idx_code)

  res <- list('stock_info' = stock_info,
              'stock_daily' = stock_daily,
              'stock_cons' = stock_cons,
              'stock_managed' = stock_managed,
              'stocks_in_index' = stocks_in_index,
              'index_daily' = index_daily)
  
  if(save){
    cols <- list(c('sym_cd','info_update'),
                 c('base_dt','sym_cd'),
                 c('num_key'),
                 c('num_key'),
                 c('num_key','idx_code'),
                 c('base_dt','idx_code'))
    pmap(list(names(res),res,cols), db_upsert)
  }
  return(res)
}

#[실행] 일별 종목정보 일괄 업데이트=======

res <- map(update_days, 
           ~get_daily_tables(.x, save=T)) %>% 
  pmap(bind_rows)


stock <- db_obj('stock_daily') %>% 
  filter(base_dt=='2022-11-30') %>% 
  left_join(
    db_obj('stock_info'),
    by=c('sym_cd', 'info_update')
  ) %>%
  left_join(
    db_obj('workdays'),
    by=c('base_dt')
  ) %>%
  left_join(
    db_obj('company_fs_bs'),
    by=c('sym_cd','fs_q')
  ) %>%
  left_join(
    db_obj('stock_cons'),
    by='num_key'
  ) %>%
  left_join(
    db_obj('company_fs_pl_prep'),
    by=c('sym_cd','fs_q')
  ) %>% collect()

samsung <- db_obj('stock_daily') %>% 
  filter(sym_cd=='005930') %>% 
  left_join(
    db_obj('workdays'),
    by=c('base_dt')
  ) %>%
  left_join(
    db_obj('company_fs_bs'),
    by=c('sym_cd','fs_q')
  ) %>%
  left_join(
    db_obj('stock_cons'),
    by='num_key'
  ) %>%
  left_join(
    db_obj('company_fs_pl_prep'),
    by=c('sym_cd','fs_q')
  ) %>% collect()

kospi <- db_obj('index_daily') %>% 
  filter(idx_code == 'P001') %>% 
  collect()
  

save(list=c('stock','samsung','kospi'), file='stock.Rdata')
load(file='stock.Rdata')
rm(stock)


unix_time =
  (Sys.time() %>% 
    as.numeric() * 1000
  ) %>%
  round() %>% 
  as.character()

otp_params <- list(
  bld = 'MKD/01/0110/01100305/mkd01100305_01',
  name = 'form',
  '_' = unix_time)

otp_url <- 'http://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
ref <- 'http://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp'
user.agent <- 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 '

otp_code <- 
  GET(url = otp_url,
      query = otp_params,
      add_headers(referer = ref),
      user_agent(user.agent)
  ) %>% 
  content('t')

otp_code

view_params <- list(
  search_bas_yy = '2022',
  gridTp = 'KRX',
  pagePath = 'contents/MKD/01/0110/01100305/MKD01100305
.jsp',
  code = otp_code)

holidays <- 
  post_krx('open',view_params)$calnd_dd %>% 
  as.Date()

view_url <- 'http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx'

res <- POST(url = view_url,
            query = view_params, 
            user_agent(user.agent), 
            add_headers(referer=ref)) %>% 
  content('t') %>% 
  fromJSON()

res[[ names(res)[1] ]] %>% 
  as_tibble()
