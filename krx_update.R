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
         fs_q = as.yearqtr(workdays - months(i)),
         fs_y = year(workdays-months(15))) %>% 
    fill(everything())
}

#[실행] DB 영업일 테이블(workdays) 갱신====
last_updated_year <- 2021 #채워넣자
this_year <- year(today())
y <- c(last_updated_year, this_year) %>% 
  unique()
map_int(y, ~db_del('workdays',
                   glue("year(base_dt) = {.x}")))
workdays <- map_dfr(y, get_workdays_from_krx)
db_upsert('workdays', workdays, 'base_dt')

  
# 
# params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01501",
#                mktId = "ALL",
#                trdDd = date)
# col_old <- c('ISU_SRT_CD', 'ACC_TRDVOL', 'ACC_TRDVAL',
#              'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 
#              'TDD_CLSPRC')
# col_new <- c('sym_cd', 'trd_vol', 'trd_val', 
#              'open', 'high', 'low', 'close')
# df1 <- post_krx('data', params) %>% 
#   select(all_of(col_old)) %>% 
#   setNames(col_new)
# 
# params <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01602",
#                mktId = 'ALL',
#                strtDd = date,
#                endDd = date,
#                adjStkPrc_check = 'Y')
# col_old <- c('ISU_SRT_CD', 'BAS_PRC')
# col_new <- c('sym_cd', 'base_p')
# df2 <- post_krx('data', params) %>% 
#   select(all_of(col_old)) %>% 
#   setNames(col_new)
# 
# df <- df1 %>% 
#   left_join(df2, by='sym_cd')%>% 
#   mutate(across(!sym_cd, parse_number)) %>% 
#   filter(!is.na(sym_cd)) %>% 
#   mutate(ret = (close / base_p) - 1,
#          base_dt = date) %>% 
#   relocate(base_dt, .before = 1)
