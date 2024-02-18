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

db_obj <- function(table) dplyr::tbl(con, table)

db_upsert <- function(table, df, cols){
  print(stringr::str_c(table," 테이블 DB 업데이트..."))
  
  dbx::dbxUpsert(conn=con, 
                 table=table, 
                 records=df, 
                 where_cols=cols)
}

db_del <- function(table,query){
  DBI::dbExecute(con, 
                 glue("delete from {table} where {query}"))
}


# 데이터베이스 쿼리 보조함수
sym_dt_pick <- function(db_tbl, .sym=NULL, .start=NULL, .end=NULL){
  
  cond1 <- NULL
  cond2 <- NULL
  cond3 <- NULL
  
  if(!is.null(.sym)) {cond1 <- '(sym_cd %in% .sym)'}
  if(!is.null(.start)){cond2 <- '(base_dt >= .start)'}
  if(!is.null(.end)){cond3 <- '(base_dt <= .end)'}
  
  cond <- c(cond1,cond2,cond3)
  
  if(!is.null(cond)){
    cond <- rlang::parse_expr(paste(cond, collapse='& '))
    db_tbl <- db_tbl |> filter(!!cond)
    return(db_tbl)
  } else{
    return(db_tbl)
  }
  
}

factor_sym_dt <- function(.sym=NULL, .start=NULL, .end=NULL){
  db_obj('stock_daily') %>% select(1:3) %>%
    sym_dt_pick(.sym, .start, .end) %>%
    left_join(
      db_obj('factor_daily'),
      by = 'num_key'
    )
}