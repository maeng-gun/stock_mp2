# 패키지 불러오기====

library(tidyverse)
library(jsonlite)
library(httr)
library(glue)
library(timetk)
library(lubridate)
library(zoo)
library(bizdays)
library(reticulate)

fn <- import('fn')

# 데이터베이스 관리====

con <- DBI::dbConnect(odbc::odbc(), 
                 "mysql_8.0", 
                 timeout = 10,
                 database = 'inp_raw',
                 bigint = 'numeric',
                 encoding = 'utf8')

db_query <- function(sql){
  DBI::dbGetQuery(con, sql) %>% as_tibble()
}

db_obj <- function(table) tbl(con, table)

db_upsert <- function(table, df, cols){
  dbx::dbxUpsert(con, table, df, cols)
}

db_del <- function(table,condition){
  DBI::dbExecute(con, 
                 glue("delete from {table} where {condition}"))
}


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
    fromJSON()
  
  res[[ names(res)[1] ]] %>% 
    as_tibble()
}

get_workdays_to_be_updated <- function(){
  last_update <- 
    db_obj("stock_daily") %>%
    select(base_dt) %>% 
    distinct() %>%
    pull() %>% max()
  
  last_workday <- 
    today() - days(
      ifelse(hour(now()) < 9, 2, 1))
  
  daily_update <- 
    db_obj("workdays") %>% 
    filter(base_dt > last_update & 
             base_dt <= last_workday) %>%
    pull(base_dt) 
  
  return(daily_update)
}



