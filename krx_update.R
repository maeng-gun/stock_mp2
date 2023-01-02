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
  
  cal <- create.calendar(
    name = 'mycal',
    holidays = holidays,
    weekdays = c('saturday','sunday'),
    start.date = start,
    end.date = end)
  
  workdays <- bizseq(start, end, cal)
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
map_int(y, ~db_del('workdays',
                   glue("year(base_dt) = {.x}")))
workdays <- map_dfr(y, get_workdays_from_krx)
db_upsert('workdays', workdays, 'base_dt')



#[함수] 특정 영업일 정보 얻기 =================

get_stock_tables <- function(yyyymmdd){
  
  print(glue('{yyyymmdd} 개별종목 일별지표 크롤링...'))
  
  #KRX정보시스템 전종목시세[12001] 페이지 정보
  params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01501",
                 mktId = "ALL",
                 trdDd = yyyymmdd)
  df1 <- post_krx('data', params)
  
  #KRX정보시스템 전종목등락률[12002] 페이지 정보
  params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01602",
                 mktId = 'ALL',
                 strtDd = yyyymmdd,
                 endDd = yyyymmdd,
                 adjStkPrc_check = 'Y')
  df2 <- post_krx('data', params) %>% 
    select(ISU_SRT_CD, BAS_PRC)

  #테이블 조인  
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
  
  #인프라펀드/선박투자/인프라투자 종목 정보
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
  
  #주요지수 구성종목 정보
  col <- c('indIdx', 'indIdx2', 'tboxindIdx_finder_equidx0_2', 'codeNmindIdx_finder_equidx0_2')
  params <- list(bld = "dbms/comm/finder/finder_equidx",
                 mktsel = '1')
  idx <- post_krx('data', params)[, c("full_code", "short_code", "codeName", "codeName")] %>% 
    setNames(col)

  get_stock_list_in_index <- function(idx_k){
  
    idx_kk <- str_replace(idx_k,' ','')
    
    tryCatch({
      params <- idx %>% 
        filter(tboxindIdx_finder_equidx0_2 == idx_k) %>% 
        mutate(bld="dbms/MDC/STAT/standard/MDCSTAT00601",
               trdDd=yyyymmdd) %>% 
        as.list()
      
      df <- post_krx("data", params) %>% 
        select(ISU_SRT_CD) %>% 
        setNames('sym_cd') %>% 
        mutate("{idx_kk}" := T)
      
      return(df)  
    }, error=function(e) {
      df <- data.frame(matrix(NA,0,2)) %>% 
        set_names(c('sym_cd',idx_kk)) %>% 
        mutate(sym_cd=as.character(sym_cd))
      return(df)
    })
  }
  
  idx_k <- c('코스피 200', '코스닥 150')
  yyyymmdd <- '20140113'
  map(idx_k, get_stock_list_in_index) %>% 
    reduce(~full_join(.x, .y, 'sym_cd'))
  
  
  
  
  b <- tibble()
  names(b) <- c('sym_cd','코스피200')
  
  left_join()

  rm(df)
    
  df_list <- list()
  col <- 
  col_new <- "sym_cd"
  
  for (index in index_list_k) {
    tryCatch({
      params <- idx[idx$tboxindIdx_finder_equidx0_2 == index,]
      params <- c(params, bld="dbms/MDC/STAT/standard/MDCSTAT00601", trdDd=date)
      df <- krx_req_post("data", params)[, col]
      names(df) <- col_new
      df[, index] <- TRUE
      df_list[[length(df_list) + 1]] <- df
    }, error=function(e) {
      cols <- c(col_new, index)
      df <- tibble((matrix(NA, 0, 2)) %>% setNames(c('가','나')))
      names(df) <- cols
      df_list[[length(df_list) + 1]] <- df
    })
  }
  
  data <- Reduce(function(x, y) merge(x, y, all=TRUE, by=col_new), df_list)
  names(data) <- c(col_new, index_list)
  
  red
  
  
  paste2 <- function(x, y, sep = ".") paste(x, y, sep = sep)
  letters[1:4] %>% reduce2(c("-", ".", "-"), paste2)
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
    
  }
  
yyyymmdd <- '20221229'

str(get_stock_tables('20221223'))

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

# 
