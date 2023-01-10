source("utils.R")

## 인덱스 구성종목 크롤링 - 일괄 업데이트====

idx_list <- 
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

fcn_idx <- function(yyyymmdd){
  
  print(paste(yyyymmdd, "인덱스 구성종목 크롤링..."))
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
    df = post_krx("data",params)
    Sys.sleep(0.05)
    if(nrow(df)!=0){
      df %>% 
        transmute(base_dt = ymd(yyyymmdd),
                  sym_cd = ISU_SRT_CD,
                  idx_code = params$idx_code)
    } else df 
  }
  df <- map_dfr(params_list, get_stocks_in_idx) 
  
  df2 <- db_obj('stock_daily') %>% 
    filter(base_dt == yyyymmdd) %>% 
    select(num_key:sym_cd) %>% 
    collect()
  
  df3 <- df %>% 
    inner_join(df2, by=c('base_dt','sym_cd')) %>% 
    select(num_key, idx_code) %>% 
    arrange(num_key, idx_code)
  
  db_upsert('stocks_in_index',df3,cols=c('num_key','idx_code'))
}

workdays <- db_obj('workdays') %>%
  filter(base_dt >= '20130917',
         base_dt <= '20221031') %>% 
  pull(base_dt) %>% strftime(format='%Y%m%d')

map_dfr(workdays, fcn_idx)

