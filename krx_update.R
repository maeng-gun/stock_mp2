#[변수] 업데이트 대상 영업일 목록=======
daily_update <- 
  db_obj('daily_update') %>% collect() %>%
  .$base_dt


user_agent <- 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 '

##[함수] KRX 사이트 크롤링(POST 방식) ====
post_krx <- function(site, params){
  
  url <- list(
    data='http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
    open='http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx')
  
  referer <- 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'
  
  res <- POST(url=url[site],
              query=params, 
              user_agent(user_agent), 
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
  
  unix_time <- 
    (as.numeric(Sys.time()) * 1000) %>% 
    round() %>% as.character()
  
  otp_params <- list(
    bld = 'MKD/01/0110/01100305/mkd01100305_01',
    name = 'form',
    '_' = unix_time)
  
  url <- 'http://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
  
  otp_code <- 
    GET(url=url,
        query=otp_params,
        user_agent(user_agent)) %>% 
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
  
  Sys.setlocale(category = 'LC_TIME',locale = 'english')
  
  workdays <- tk_make_weekday_sequence(
    year,
    remove_weekends = TRUE,
    skip_values = holidays)
  
  Sys.setlocale(category = 'LC_TIME',locale='Korean_Korea.utf8')

  i <- ifelse(month(workdays)==3, 6, 5)
  
  tibble(base_dt = workdays,
         fs_q = as.yearqtr(workdays - months(i)),
         fs_y = year(workdays-months(15))) %>% 
    fill(everything())
}

#[실행] DB 영업일 테이블(workdays) 갱신====
year <- str_sub(daily_update, end=4) %>% 
  unique() %>% as.integer() %>% 
  c(., last(.)+1)

map_int(year, ~db_del('workdays', 
                      glue("year(base_dt) = {.x}")))

workdays <- map_dfr(year, get_workdays_from_krx)

db_upsert('workdays_2', 
          workdays, 
          'base_dt')
DBI::dbWriteTable(con, "workdays_2",workdays)
db_obj("workdays_2") %>% collect() %>% 
  arrange(desc(fs_q),desc(base_dt))
  
  
library(zoo)
# 
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
