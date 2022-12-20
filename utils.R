library(tidyverse)
library(magrittr)
library(glue)
library(zeallot)
library(jsonlite)
library(httr)
library(lubridate)
library(tsibble)
library(bizdays)

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
