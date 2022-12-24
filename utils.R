library(tidyverse)
library(jsonlite)
library(httr)
library(glue)
library(timetk)
library(lubridate)
library(zoo)
library(bizdays)

# library(magrittr)
# library(zeallot)




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


# 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
git config --global user.email "sarangiason@naver.com"
git config --global user.name "maeng-gun"
