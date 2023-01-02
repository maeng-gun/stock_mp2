##[함수] KRX 사이트 크롤링(POST 방식) ====

post_krx <- function(site, params){
  
  url <- list(
    data='http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
    open='http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx')

  user.agent <- 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 '
  referer <- 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'
  
  res <- POST(url=url[site],
              query=params, 
              user_agent(user.agent), 
              add_headers(referer=referer)) %>% 
    content('t') %>% 
    fromJSON() %>% 
    {. ->> res_json} %>% 
    .[[ names(res_json)[1] ]] %>% 
    as_tibble()
  
  return(res)
}

#[함수] 특정 연도 영업일 정보 얻기 =================
get_workdays_from_krx <- function(year){
  
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
  
  mycal <- create.calendar(
    name = 'mycal',
    holidays = holidays,
    weekdays = c('saturday','sunday'),
    start.date = start,
    end.date = end)
  
  workdays <- bizseq(from = start, to = end, cal=mycal)
  i <- ifelse(month(workdays)==3, 6, 5)
  
  tibble(base_dt = workdays,
         fs_q = (workdays - months(i)) %>% 
           as.yearqtr() %>% 
           as.character(),
         fs_y = (workdays-months(15)) %>% 
           year()) %>% 
    fill(everything())
}



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


#[함수] 특정 영업일 정보 얻기 =================

get_stock_tables <- function(yyyymmdd){
  
  print(glue('{yyyymmdd} 개별종목 일별지표 크롤링...'))
  
  #KRX정보시스템 전종목시세[12001] 페이지 정보====
  params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01501",
                 mktId = "ALL",
                 trdDd = yyyymmdd)
  df1 <- post_krx('data', params)
  
  #KRX정보시스템 전종목등락률[12002] 페이지 정보====
  params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01602",
                 mktId = 'ALL',
                 strtDd = yyyymmdd,
                 endDd = yyyymmdd,
                 adjStkPrc_check = 'Y')
  df2 <- post_krx('data', params) %>% 
    select(ISU_SRT_CD, BAS_PRC)

  #테이블 조인====
  col_old <- c('ISU_SRT_CD', 'ACC_TRDVOL', 'ACC_TRDVAL', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'BAS_PRC', 'ISU_ABBRV', 'MKT_ID')
  col_new <- c('sym_cd', 'trd_vol', 'trd_val', 'open', 'high', 'low', 'close', 'base_p', 'sym_nm', 'mkt_cd')
  
  df1 <- df1 %>% 
    left_join(df2, by='ISU_SRT_CD')%>% 
    select(all_of(col_old)) %>%
    setNames(col_new) %>% 
    mutate(across(!c(sym_cd,sym_nm,mkt_cd), parse_number)) %>%
    filter(!is.na(sym_cd)) %>%
    mutate(ret = (close / base_p) - 1,
           base_dt = ymd(yyyymmdd)) %>%
    relocate(base_dt, .before = 1)
  
  #인프라펀드/선박투자/인프라투자 종목 정보====
  blds = list(p12014 = "dbms/MDC/STAT/standard/MDCSTAT02901",
              p12015 = "dbms/MDC/STAT/standard/MDCSTAT02801",
              p12016 = "dbms/MDC/STAT/standard/MDCSTAT03001")
  read <- function(bld){
    params <- list(bld = blds[bld],
                  trdDd = yyyymmdd)
    data <- post_krx('data', params) %>% 
      transmute(sym_cd = ISU_SRT_CD,
                inv_com = TRUE)
    return(data)}
  df2 <- map_dfr(names(blds), read)
  
  # 인덱스 구성종목 정보====
  
  idx_list <- 
    post_krx('data', 
             list(bld = "dbms/comm/finder/finder_equidx",
                  mktsel = '1')) %>% 
    mutate(
      mkt_nm = map_chr(marketName,
                       ~switch(.x, "KRX"="X",
                                   "KOSPI"="P",
                                   "KOSDAQ"="Q",
                                   "테마"="T")),
      .before=short_code) %>% 
    unite('idx_code', mkt_nm:short_code,
          sep='', remove=F) %>% 
    select(-marketCode,-marketName)
  
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
    print(params[[3]])
    df = post_krx("data",params)
    if(nrow(df)!=0){
      df %>% 
        transmute(base_dt = yyyymmdd,
                  idx_code = params$idx_code,
                  sym_cd = ISU_SRT_CD)
    } else df 
  }
  df <- map_dfr(params_list,
                get_stocks_in_idx)
  # dbWriteTable(con,'stocks_in_index',df, append=TRUE)
  
  # 인덱스 가격 정보====
  
  mkt_nm_list <- c("01" = "X", "02" = "P", "03" = "Q", "04" = "T")
  
  get_index_table <- function(mkt){
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
  
  df <- map_dfr(names(mkt_nm_list), get_index_table) %>% 
    arrange(idx_code)

}

#[실행] DB 일별 테이블(daily) 업데이트  ====
yyyymmdd <- '20221229'

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

# 인덱스 구성종목 정보 가로피벗
db_obj('stocks_in_index') %>% 
  collect() %>% 
  mutate(value=T) %>% 
  pivot_wider(names_from = idx_code,
              values_from = value) %>% 
  arrange(sym_cd)


