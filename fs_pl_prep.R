print("기존종목에 대한 PL 테이블 전처리 시작") #------------

db_pl <- # PL테이블 쿼리 생성
  db_obj('company_fs_bs') %>% 
  select(sym_cd:quarter) %>% 
  left_join(
    db_obj('company_fs_pl') %>% 
      select(-dpr,-prop_div))

updated_quarter <- # 최근 기업재무자료 업데이트된 분기 목록
  db_obj('updated_quarter') %>% collect()

yrqtr <- function(row){
  as.character(row) %>% paste(collapse="")
}

c(start_1y, start, end) %<-% (
  updated_quarter %>% 
  slice(c(1,n())) %>% 
  add_row(year=.$year[1]-1, 
          quarter=1, 
          .before=1) %>% 
  apply(1, yrqtr))
  

df <- #전년1분기 이후 PL테이블 쿼리
  db_pl %>% filter(
    between(paste0(year,quarter), start_1y, end)
    )

## 손익지표 전처리 함수 --------------------------------------
preprocess_fs_pl <- function(df){
  
  df_info <- #df에서 종목/분기 정보만
    df %>% select(sym_cd:quarter) %>% 
    collect()
  
  df_raw <- # df에서 손익지표 컬럼만
    df %>% select(-sym_cd:-quarter) %>% 
    collect()
  
  df_1y <- # df에 대한 1년 lag
    df %>% filter(quarter==4) %>% 
    select(-quarter) %>% 
    mutate(year=year+1) %>% 
    right_join(
      df %>% select(sym_cd:quarter)
    ) %>%
    arrange(sym_cd,year,quarter) %>% 
    select(-sym_cd,-year,-quarter) %>% 
    collect()
  
  df_4q <- #df에 대한 4분기 lag(전년동기)
    df %>% group_by(sym_cd) %>%
    mutate(across(-year:-quarter, 
                     ~lag(.,n=4L))) %>%
    ungroup() %>% 
    select(-sym_cd:-quarter) %>% collect()
  
  df_a4q <- # 직전 4개분기 누적(gross) 손익지표
    (df_raw+df_1y-df_4q) %>% 
    map2(df_1y, ~ifelse(is.na(.x), .y, .x)) %>% #직전 4개분기 중 결측값이 있는 경우라면 전년말 지표로 대체
    bind_cols(df_info,.)
  
  #원자료에서 연도말 지표가 있다면 대체
  df_a4q[df_a4q$quarter==4,] <- df[df$quarter==4,] 
  
  
  df_1q <- #df에 대한 1분기 lag(전분기)
    df %>% group_by(sym_cd, year) %>%
    mutate(across(-quarter,
                  ~lag(., n = 1L, default = 0L))) %>% 
    ungroup() %>% 
    select(-sym_cd:-quarter) %>% 
    collect()
  
  df_n <- #순(net) 분기지표
    bind_cols(df_info, df_raw - df_1q)
  
  df_final <- 
    df_a4q %>% left_join(
      df_n,
      by = names(df_info),
      suffix = c("_a4q","_n")
    ) %>% 
    mutate(earn_a4q = ifelse(is.na(earn_dom_a4q), 
                             earn_a4q,
                             earn_dom_a4q),
           earn_n = ifelse(is.na(earn_dom_n),
                           earn_n,
                           earn_dom_n)
    ) %>% 
    select(-earn_dom_a4q, -earn_dom_n)
  
  return(df_final)
}
## ---------------------------------------

df_old <- # 기존 종목 전처리 테이블_최종
  preprocess_fs_pl(df) %>% filter(
    paste0(year, quarter) %>% 
    between(start, end)
  )

db_upsert('company_fs_pl_prep', 
          df_old, 
          c('sym_cd', 'year','quarter'))

print("신규종목에 대한 PL 테이블 전처리 시작")#-------------


new_symbols <- ## 신규종목 목록
  db_obj('new_symbols') %>% collect() %>% 
  .$sym_cd

if (length(new_symbols)!=0){
  
  c(start, end) %<-% (  #기업재무자료 최초 분기
    db_obj('company_fs_bs') %>% 
    select(year, quarter) %>%
    arrange(year, quarter) %>% 
    distinct(year, quarter) %>% collect() %>% 
    slice(c(1,n())) %>% apply(1,yrqtr))
  
  df_new <- #전년1분기 이후 PL테이블 쿼리
    db_pl %>% filter(
      sym_cd %in% new_symbols
    ) %>% collect() %>% 
    preprocess_fs_pl()
  
  db_upsert('company_fs_pl_prep',
            df_new, 
            c('sym_cd', 'year','quarter'))
}

