#[변수] fnguide 크롤링 DB정보 얻기====
fn_tables <- db_obj('fn_tables') |> 
  collect() |> 
  mutate(across(numeric:pl, as.logical))

#[클래스] 엑셀 자동화====
reticulate::source_python('fn.py')

# [함수] Fn가이드 횡단면데이터 크롤링====
get_cross_section_data <- 
  function(symbols, fs=NULL, daily=NULL, new=FALSE){
    symbols <- paste0('A', symbols)
    t <- if(!is.null(fs)) 'fs' else 'daily'
    
    tbl_info <- fn_tables %>% filter(table == t)
    tbl_info <- if(new) tbl_info %>% filter(new) else tbl_info
    num_cols <- tbl_info %>% 
      filter(numeric) %>% pull(item)
    
    ft <- FnTools('cs_data.xlsm')
    ft$paste('A7', symbols, down=TRUE)
    ft$paste('C2', tbl_info$item_kind)
    ft$paste('C5', tbl_info$item_code)
    ft$paste('C6', tbl_info$item_nm)
    
    if(!is.null(fs)){
      item_freq <- list('Q1' = '1st-Quarter', 
                        'Q2' = 'Semi-Annual',
                        'Q3' = '3rd-Quarter', 
                        'Q4' = 'Annual')
      y <- str_sub(fs,end=4)
      q <- str_sub(fs,6)
      
      ft$paste('C3', rep(item_freq[[q]], nrow(tbl_info)))
      ft$paste('C4', rep(y, nrow(tbl_info)))
      ft$refresh()
      ft$paste('A6', c('sym_cd','fs_q', tbl_info$item))
      
      df <- ft$to_df("A6") %>% 
        map_dfc(as.character) %>% 
        mutate(fs_q = fs)
      
    } else {
    
      ft$paste('C3', tbl_info$item_freq)
      ft$paste('C4', rep(daily, nrow(tbl_info)))
      ft$refresh()
      ft$paste('A6', c('sym_cd','base_dt', tbl_info$item))
      
      df <- ft$to_df("A6") %>% 
        map_dfc(as.character) %>% 
        mutate(base_dt = ymd(daily)) %>% 
        relocate(base_dt, .before=1)
    }
    ft$close()
    df <- df %>% 
      mutate(across(all_of(num_cols), 
                    ~readr::parse_number(.x, na=c("NA","","NaN","NULL","N/A")))) %>%
      mutate(sym_cd = str_sub(sym_cd, 2))
    
    return(df)
  }

# [함수] Fn가이드 분기데이터 크롤링====
get_fiscal_basis_data <- 
  function(symbols, year_range, new=FALSE){
    
    symbols <- paste0('A', symbols)
    tbl_info <- fn_tables %>% filter(table == 'fs')
    tbl_info <- if(new) tbl_info %>% 
      filter(new) else tbl_info
    num_cols <- tbl_info %>% 
      filter(numeric) %>% pull(item)
    
    ft <- FnTools('fb_data.xlsm')
    ft$paste('A12', symbols, down=T)
    ft$paste('F8', tbl_info$item_kind)
    ft$paste('F9', tbl_info$item_code)
    ft$paste('F10', tbl_info$item_nm)
    ft$paste('B7', year_range)
    ft$refresh()
    
    item_name <- c('sym_cd','fs_q','month',
                   'year', 'quarter', tbl_info$item)
    ft$paste('A11',item_name)
    
    freq = c('1st-Quarter' = 'Q1', 
             'Semi-Quarter' = 'Q2', 
             '3rd-Quarter' = 'Q3', 
             'Annual' = 'Q4')
    
    df <- ft$to_df("A11") %>% 
      map_dfc(as.character) %>% 
      mutate(sym_cd = str_sub(sym_cd, 2),
             fs_q = paste(year, 
                          str_replace_all(quarter, freq))) %>% 
      mutate(across(all_of(num_cols), 
                    ~parse_number(.x, 
                                  na=c("NA","","NaN","NULL","N/A")))) %>%
      select(-month:-quarter)
    
    ft$close()
    
    return(df)
  }


##[함수] KRX 사이트 크롤링(POST 방식) ====

post_krx <- function(site, params){
  
  url <- list(
    data='http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
    open='http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx')
  
  user.agent <- 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 '
  referer <- 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'
  
  res <- httr::POST(url=url[site],
                    query=params, 
                    user_agent(user.agent), 
                    add_headers(referer=referer)) %>% 
    content('t') %>% 
    jsonlite::fromJSON()
  
  res[[ names(res)[1] ]] %>% 
    as_tibble()
}




# FnTools <- function(file_name){
#   wb <- xw$Book(paste0(getwd(),'/util/',file_name))
#   ws <- wb$sheets(1)
#   
#   close <- function() wb$app$quit()
#   
#   identify_range <- function(range_name) ws[range_name]
#   
#   clear_table <- function(base_cell) {
#     identify_range(base_cell)$expand('table')$clear()
#   }
#   
#   paste <- function(base_cell, value, down=FALSE) {
# 
#     if (is.vector(value)){
#       if(down){
#         value <- matrix(value,ncol=1)
#       } else {
#         value <- matrix(value,nrow=1)
#       }
#     } else {
#       value <- as.matrix(value) 
#     }
#     try(identify_range(base_cell)$options(np$ndarray)$value <- value, 
#         silent = T)
#   }
#   
#   to_df <- function(base_cell='A1', cols=NULL) {
#     return(identify_range(base_cell)$options(pd$DataFrame, expand='table', index=FALSE)$value)
#   }
#   
#   refresh <- function() {
#     wb$macro("DoAllSheetRefresh")()
#   }
#   
#   list(wb=wb, ws=ws, close=close, 
#        identify_range=identify_range, 
#        clear_table=clear_table, 
#        paste=paste, to_df=to_df, refresh=refresh)
# }
