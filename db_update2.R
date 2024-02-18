#[변수] 업데이트 대상 기간 얻기====
stocks_list <- #DB 내 전종목
  db_obj('stock_info') %>% 
  distinct(sym_cd) %>%
  arrange(sym_cd) %>% 
  collect()

last_updated_date <- #마지막 업데이트일 
  db_obj('stock_daily') %>%
  summarise(base_dt=max(base_dt, na.rm=T)) %>% 
  pull()

last_date <- #전일 또는 전전일
  today() - 
  (ifelse(hour(now()) < 9, 2, 1) %>% days())

days_2b_updated <- #업데이트가 필요한 일자
  db_obj('workdays') %>% 
  filter(base_dt > last_updated_date,
         base_dt <= last_date) %>% 
  collect() %>% pull(base_dt) %>% 
  strftime('%Y%m%d')

years_2b_updated <- #업데이트가 필요한 연도
  last_updated_date %>% year() %>% 
  c(., year(today())) %>% unique()

  
#[함수] 연도별 영업일 정보 얻기====
get_workdays_from_krx <- function(year, save=F){
  
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

  cal <- bizdays::create.calendar(
    name = 'mycal',
    holidays = holidays,
    weekdays = c('saturday','sunday'),
    start.date = start,
    end.date = end)
  
  workdays <- bizdays::bizseq(start, end, cal)
  i <- ifelse(month(workdays)==3, 6, 5)
  
  res <- 
    tibble(base_dt = workdays,
           fs_q = (workdays - months(i)) %>% 
             zoo::as.yearqtr() %>% 
             as.character(),
           fs_y = (workdays-months(15)) %>% 
             year()) %>% 
    fill(everything())
  
  if(save){
    db_del(table = 'workdays',
           query = glue("year(base_dt) = {year}"))
    
    db_upsert(table = 'workdays', 
              df = res, 
              col = 'base_dt')
  }
  return(res)
}


# #[실행] workdays 테이블 업데이트====
# workdays <- map_dfr(years_2b_updated, 
#                     ~get_workdays_from_krx(.x, save=F))


#[실행] index_list 테이블 업데이트====
index_list <-
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

db_upsert('index_list', index_list, cols=c('idx_code','codeName'))

#[함수] 일별 종목 정보 얻기 =================
get_daily_tables <- function(yyyymmdd, save=F){
  print(glue('{yyyymmdd} 개별종목 일별지표 크롤링...'))
  print('- KRX 정보시스템 크롤링...')
  #개별종목 가격정보
  params1 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01501",
                  mktId = "ALL",
                  trdDd = yyyymmdd)
  df1 <- post_krx('data', params1)
  
  params2 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT01602",
                  mktId = 'ALL',
                  strtDd = yyyymmdd,
                  endDd = yyyymmdd,
                  adjStkPrc_check = 'Y')
  df2 <- post_krx('data', params2) %>% 
    select(ISU_SRT_CD, BAS_PRC)
  
  col_old <- c('ISU_SRT_CD', 'ACC_TRDVOL', 'ACC_TRDVAL', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'BAS_PRC', 'ISU_ABBRV', 'MKT_ID')
  col_new <- c('sym_cd', 'trd_vol', 'trd_val', 'open', 'high', 'low', 'close', 'base_p', 'sym_nm', 'mkt_cd')
  
  df3 <-
    left_join(df1,df2, by='ISU_SRT_CD')[col_old] %>%
    setNames(col_new) %>% 
    mutate(across(!c(sym_cd,sym_nm,mkt_cd), parse_number)) %>%
    filter(!is.na(sym_cd)) %>%
    mutate(base_dt = ymd(yyyymmdd),
           base_p = ifelse(base_p==1, NA, base_p)) %>%
    relocate(base_dt, .before = 1) %>% 
    filter( mkt_cd!='KNX',
            str_detect(sym_cd, '0$'),
            !str_detect(sym_cd, '^9'),
            !str_detect(sym_nm, '스팩'),
            !is.na(base_p))
  
  #인프라펀드/선박투자/인프라투자 종목 정보
  blds = list(p12014 = "dbms/MDC/STAT/standard/MDCSTAT02901",
              p12015 = "dbms/MDC/STAT/standard/MDCSTAT02801",
              p12016 = "dbms/MDC/STAT/standard/MDCSTAT03001")
  fcn_inv <- function(bld){
    params <- list(bld = blds[bld],
                   trdDd = yyyymmdd)
    data <- post_krx('data', params) %>% 
      transmute(sym_cd = ISU_SRT_CD)
    return(data)}
  df4 <- map_dfr(names(blds), fcn_inv)
  
  #KRX 섹터정보
  sector_match = c('^음식료·담배$' = '음식료품', 
                   '^섬유·의류$' = '섬유의복', 
                   '^종이·목재$' = '종이목재',
                   '^출판·매체복제$' = '종이목재', 
                   '^제약$' = '의약품', 
                   '^비금속$' = '비금속광물', 
                   '^금속$' = '철강금속',
                   '^기계·장비$' = '기계', 
                   '^일반전기전자$' = '전기전자', 
                   '^의료·정밀기기$' = '의료정밀',
                   '^운송장비·부품$' = '운수장비', 
                   '^기타제조$' = '기타제조업', 
                   '^전기·가스·수도$' = '전기가스업',
                   '^건설$' = '건설업', 
                   '^유통$' = '유통업', 
                   '^숙박·음식$' = '서비스업', 
                   '^운송$' = '운수창고업',
                   '^금융$' = '기타금융', 
                   '^기타서비스$' = '서비스업', 
                   '^오락·문화$' = '서비스업', 
                   '^통신서비스$' = '통신업',
                   '^방송서비스$' = '서비스업', 
                   '^인터넷$' = '서비스업', 
                   '^디지털컨텐츠$' = '서비스업', 
                   '^소프트웨어$' = '서비스업',
                   '^컴퓨터서비스$' = '서비스업', 
                   '^통신장비$' = '전기전자', 
                   '^정보기기$' = '전기전자', 
                   '^반도체$' = '전기전자',
                   '^IT부품$' = '전기전자')
  
  fcn_sec = function(mkt){
    params = list(bld = "dbms/MDC/STAT/standard/MDCSTAT03901",
                  mktId = mkt,
                  trdDd = yyyymmdd)
    data = post_krx("data", params)
    if(nrow(data) > 0){
      col = c("ISU_SRT_CD", "IDX_IND_NM")
      col_new = c("sym_cd", "sec_krx")
      data = data[col] %>% 
        set_names(col_new) %>% 
        mutate(sec_krx = str_replace_all(sec_krx,sector_match))
    }
    return(data)
  }
  
  df5 <- map_dfr(c('STK','KSQ'), fcn_sec)
  
  #krx-data 기초자료
  krx_data <- df3  %>% 
    anti_join(df4, by='sym_cd') %>% 
    left_join(df5, by='sym_cd') %>% 
    arrange(sym_cd)
  
  #fn가이드 기초자료
  print('- DataGuide 크롤링...')
  fn_data <- 
    get_cross_section_data(symbols = krx_data$sym_cd, 
                           daily = yyyymmdd)
  
  #stock_info 테이블 생성====
  print('- stock_info 테이블 생성...')
  stock_info <- db_obj('stock_info') %>% collect() %>% 
    bind_rows(
      krx_data %>% 
        select(sym_cd, base_dt, sym_nm, 
               mkt_cd, sec_krx) %>% 
        left_join(
          fn_data %>% select(sym_cd, base_dt, sec_fn_1, 
                             sec_fn_2, sec_fn_3),
          by = c('base_dt','sym_cd')) %>% 
        rename('info_update'='base_dt')) %>% 
    distinct(across(-info_update), .keep_all = T)

  #stock_daily 테이블 생성====
  print('- stock_daily 테이블 생성...')
  max_key <- db_obj('stock_daily') %>% 
    
    summarise(a=max(num_key, na.rm=T)) %>% pull()
  
  stock_daily <- krx_data %>% 
    select(base_dt:base_p) %>% 
    left_join(
      fn_data %>% select(base_dt,sym_cd,
                         net_buy_fi:num_stock_t),
      by=c('base_dt','sym_cd')) %>% 
    left_join(
      stock_info %>%
        select(sym_cd, info_update) %>% 
        group_by(sym_cd) %>% 
        slice(n()),
      by='sym_cd') %>% 
    mutate(num_key = max_key + 1:n(),
           .before=1)

  stock_info <- 
    stock_info %>% filter(info_update==ymd(yyyymmdd))
  
  #stock_cons 테이블 생성====
  print('- stock_cons 테이블 생성...')
  stock_cons <- stock_daily %>% 
    select(num_key:sym_cd) %>% 
    right_join(
      fn_data %>% 
        select(base_dt, sym_cd, 
               sales_12mf:eps_down_fy2) %>% 
        filter(!if_all(sales_12mf:eps_down_fy2, is.na)),
      by=c('base_dt','sym_cd')) %>% 
    select(!(base_dt:sym_cd))

  #stock_managed 테이블 생성====
  print('- stock_managed 테이블 생성...')
  stock_managed <- stock_daily %>% 
    select(num_key:sym_cd) %>% 
    right_join(fn_data %>% select(base_dt:sym_reg),
               by = c("base_dt", "sym_cd")) %>% 
    pivot_longer(sym_mng:sym_reg, 
                 names_to = 'managed',
                 values_to = 'value') %>% 
    filter(value!='정상') %>% 
    transmute(
      num_key = num_key,
      type = as.integer(str_replace_all(
        managed, c('sym_mng' = '1', 
                   'sym_stop'='2', 
                   'sym_reg'='3'))))
  
  # stocks_in_index 테이블 생성====
  print("- stocks_in_index 테이블 생성")
  index_list <- 
    db_obj('index_list') %>% collect()
  
  params_list <- index_list %>% 
    transmute(
      indIdx = full_code, 
      indIdx2 = short_code, 
      tboxindIdx_finder_equidx0_2 = codeName, 
      codeNmindIdx_finder_equidx0_2 = codeName,
      idx_code = idx_code,
      bld = "dbms/MDC/STAT/standard/MDCSTAT00601",
      trdDd = yyyymmdd) %>%
    apply(1,as.list)
  
  get_stocks_in_index <- function(params){
    df = post_krx("data",params)
    Sys.sleep(0.05)
    if(nrow(df)!=0){
      df %>% 
        transmute(base_dt = ymd(yyyymmdd),
                  sym_cd = ISU_SRT_CD,
                  idx_code = params$idx_code)
    } else df 
  }
  
  stocks_in_index <- 
    stock_daily %>% select(num_key:sym_cd)%>% 
    inner_join(
      map_dfr(params_list, get_stocks_in_index), 
      by=c('base_dt','sym_cd')) %>% 
    select(num_key, idx_code) %>% 
    arrange(num_key, idx_code)
  
  # index_daily 테이블 생성====
  print("- index_daily 테이블 생성")
  mkt_nm_list <- c("01" = "X", "02" = "P", "03" = "Q", "04" = "T")
  
  get_index_daily <- function(mkt){
    params1 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT00101",
                    idxIndMidclssCd = mkt,
                    trdDd = yyyymmdd,
                    share = "1",
                    money = "1")
    params2 <- list(bld = "dbms/MDC/STAT/standard/MDCSTAT00201",
                    idxIndMidclssCd = mkt,
                    strtDd = yyyymmdd,
                    endDd = yyyymmdd)
    df1 <- post_krx("data", params1)
    df2 <- post_krx("data", params2)
    if (nrow(df1)!=0) {
      col1 <- c("IDX_NM", "ACC_TRDVOL", "ACC_TRDVAL", "OPNPRC_IDX", 
                "HGPRC_IDX", "LWPRC_IDX", "CLSPRC_IDX")
      col_new1 <- c("sym_nm", "trd_vol", "trd_val", "open", 
                    "high", "low", "close")
      col2 <- c("IDX_IND_NM", "OPN_DD_INDX")
      col_new2 <- c("sym_nm", "base_p")
      
      df <- df1[col1] %>% set_names(col_new1) %>% 
        left_join(df2[col2] %>% set_names(col_new2),
                  by="sym_nm") %>% 
        filter(if_all(-sym_nm, ~.!="-")) %>% 
        mutate(across(!sym_nm, parse_number),
               base_dt = yyyymmdd) %>% 
        left_join(index_list %>% 
                    filter(mkt_nm==mkt_nm_list[mkt]) %>% 
                    select(idx_code,codeName),
                  by = c("sym_nm" = "codeName")) %>% 
        relocate(c(base_dt,idx_code)) %>% 
        select(-sym_nm)
      
      return(df)
    } else df1
  }
  
  index_daily <- 
    map_dfr(names(mkt_nm_list), get_index_daily) %>% 
    arrange(idx_code)

  res <- list('stock_info' = stock_info,
              'stock_daily' = stock_daily,
              'stock_cons' = stock_cons,
              'stock_managed' = stock_managed,
              'stocks_in_index' = stocks_in_index,
              'index_daily' = index_daily)
  
  if(save){
    cols <- list(c('sym_cd','info_update'),
                 c('base_dt','sym_cd'),
                 c('num_key'),
                 c('num_key'),
                 c('num_key','idx_code'),
                 c('base_dt','idx_code'))
    pmap(list(names(res),res,cols), db_upsert)
  }
  return(res)
}

#[실행] 일별 종목정보 일괄 업데이트=======
# res_daily <- map(days_2b_updated, 
#                  ~get_daily_tables(.x, save=T)) %>%
#   pmap(bind_rows)

#[변수] 업데이트 대상 분기 얻기====
new_stocks_list <- 
  db_obj('stock_info') %>% 
  distinct(sym_cd) %>% 
  collect() %>% 
  anti_join(stocks_list, 
            by='sym_cd')

last_updated_date <- last(days_2b_updated)

last_updated_quarter <- #마지막 업데이트월
  db_obj('company_fs_bs') %>%
  summarise(q=max(fs_q, na.rm=T)) %>% pull(q)

quarters_2b_updated <-
  db_obj('workdays') %>%
  filter(fs_q >= last_updated_quarter,
         base_dt <= last_updated_date) %>% 
  distinct(fs_q) %>% 
  collect()

#[실행] 기존종목 재무제표 테이블 업데이트====
get_company_fs_tables.old_sym <- 
  function(fs_q, save=F){
    print(glue("기존종목 {fs_q} 재무제표 테이블 생성"))
    
    col_bs <- fn_tables %>% 
      filter(table == 'fs', bs) %>% 
      pull(item)
    
    col_pl <- fn_tables %>% 
      filter(table == 'fs', pl) %>% 
      pull(item)
    
    df <- get_cross_section_data(
      symbols = pull(stocks_list),
      fs = fs_q)
    
    df_bs <- df[c('sym_cd', 'fs_q', col_bs)] %>% 
      filter(!if_all(-sym_cd:-fs_q, is.na))
    
    df_pl <- df[c('sym_cd', 'fs_q', col_pl)] %>% 
      filter(!if_all(-sym_cd:-fs_q, is.na))
    
    res <- list(old_company_fs_bs = df_bs,
                old_company_fs_pl = df_pl)
    
    if(save){
      cols <- list(c('sym_cd','fs_q'),
                   c('sym_cd','fs_q'))
      table_names <- c('company_fs_bs','company_fs_pl')
      pmap(list(table_names, res, cols), db_upsert)
    }
    return(res)
  }
# 
# res_q_old <- map(pull(quarters_2b_updated),
#                  ~get_company_fs_tables.old_sym(.x, save=T)) %>%
#   pmap(bind_rows)
# 

#[실행] 기존종목 재무제표 '전처리' 테이블 업데이트====

preprocess_fs_pl <- function(df){
  
  df_info <- #df에서 종목/분기 컬럼만
    df %>% select(sym_cd:fs_q)
  
  df_raw <- # df에서 손익지표 컬럼만
    df %>% select(-sym_cd:-fs_q)
  
  df_1y <- # df에 대한 1년 lag(전년 연간지표)
    df_info %>% 
    mutate(year=year(fs_q)) %>% 
    left_join(
      df %>% filter(quarter(fs_q)==4) %>% 
        mutate(year=year(fs_q)+1) %>% 
        select(-fs_q)
      ,by = c('sym_cd','year')
    ) %>%
    arrange(sym_cd,fs_q) %>% 
    select(-sym_cd,-fs_q,-year)
  
  df_4q <- #df에 대한 4분기 lag(전년동기)
    df %>% group_by(sym_cd) %>%
    mutate(across(-fs_q, 
                  ~lag(.,n=4L))) %>%
    ungroup() %>% 
    select(-sym_cd:-fs_q)
  
  df_a4q <- # 직전 4개분기 누적(gross) 손익지표
    tibble(df_raw+df_1y-df_4q) %>% 
    map2(df_1y, ~ifelse(is.na(.x), .y, .x)) %>% #직전 4개분기 중 결측값이 있는 경우라면 전년말 지표로 대체
    bind_cols(df_info,.)
  
  #원자료에서 연도말 지표가 있다면 대체
  df_a4q[quarter(df_a4q$fs_q)==4,] <- 
    df %>% filter(quarter(fs_q)==4)
  
  df_1q <- #df에 대한 1분기 lag(전분기)
    df %>% 
    mutate(year=year(fs_q)) %>% 
    group_by(sym_cd, year) %>%
    mutate(across(-fs_q,
                  ~lag(., n = 1L, default = 0L))) %>% 
    ungroup() %>% 
    select(!c(sym_cd,fs_q,year))
  
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
    select(-earn_dom_a4q, -earn_dom_n) %>% 
    filter(!if_all(-sym_cd:-fs_q, is.na)) %>% 
    mutate(fs_q = as.character(fs_q))
  
  
  return(df_final)
}


df_old_prep <- function(save=F){
  
  yqtr <- 
    quarters_2b_updated %>% 
    mutate(fs_q = as.yearqtr(fs_q)) %>% 
    slice(1,n()) %>% 
    add_row(fs_q = yearqtr(year(.$fs_q[1])-1),
            .before=1) %>%
    pull() %>% 
    as.character()
  
  start_1y <- yqtr[1]
  start <- yqtr[2]
  end <- yqtr[3]
  
  df_old <- #전처리 대상 PL 테이블(전년1분기~최근분기)
    db_obj('company_fs_bs') %>%
    expand(sym_cd, fs_q) %>%
    filter(fs_q >= start_1y,
           fs_q <= end) %>%
    left_join(
      db_obj('company_fs_pl') %>%
        select(-dpr,-prop_div),
      by = c("sym_cd", "fs_q")) %>%
    arrange(sym_cd, fs_q) %>%
    collect() %>%
    mutate(fs_q = as.yearqtr(fs_q))
  
  res <- 
    preprocess_fs_pl(df_old) %>%
    filter(fs_q >= start,
           fs_q <= end)
  
  if(save){
    db_upsert('company_fs_pl_prep',
              df_old_prep,
              c('sym_cd', 'fs_q'))
  }
  
  return(res)
}
  




#[실행] 신규종목 재무제표 테이블 업데이트====

get_company_fs_tables.new_sym <- 
  function(save=F){
    
    all_fs_q <- 
      db_obj('company_fs_bs') %>% 
      distinct(fs_q) %>% 
      arrange(fs_q) %>% 
      collect()
    
    start_end <- 
      all_fs_q %>%
      slice(1,n())
    
    year_range = start_end %>% 
      pull() %>% str_sub(end=4)
    print(glue("신규종목 {paste(year_range, collapse = ' ~ ')} 재무제표 테이블 생성"))
    
    col_bs <- fn_tables %>% 
      filter(table == 'fs', bs) %>% 
      pull(item)
    
    col_pl <- fn_tables %>% 
      filter(table == 'fs', pl) %>% 
      pull(item)
    
    df <- get_fiscal_basis_data(
      symbols = pull(new_stocks_list),
      year_range = year_range)
    
    df_bs <- df[c('sym_cd', 'fs_q', col_bs)] %>% 
      filter(!if_all(-sym_cd:-fs_q, is.na)) %>% 
      semi_join(all_fs_q, by='fs_q')
    
    df_pl <- df[c('sym_cd', 'fs_q', col_pl)] %>% 
      filter(!if_all(-sym_cd:-fs_q, is.na)) %>% 
      semi_join(all_fs_q, by='fs_q')
    
    res <- list(new_company_fs_bs = df_bs,
                new_company_fs_pl = df_pl)
    
    if(save){
      table_names <- c('company_fs_bs','company_fs_pl')
      cols <- list(c('sym_cd','fs_q'),
                   c('sym_cd','fs_q'))
      pmap(list(table_names,res,cols), db_upsert)
    }
    return(res)
  }
# 
# if(nrow(new_stocks_list)>0){
#   res_q_new <- get_company_fs_tables.new_sym(save=T)
# }


#[실행] 신규종목 재무제표 '전처리' 테이블 업데이트====
# 
# new_symbols <- pull(new_stocks_list)

df_new <- #전년1분기 이후 PL테이블 쿼리
  db_obj('company_fs_bs') %>% 
  expand(sym_cd, fs_q) %>% 
  filter(sym_cd %in% new_symbols) %>% 
  left_join(
    db_obj('company_fs_pl') %>% 
      select(-dpr,-prop_div),
    by = c("sym_cd", "fs_q")) %>% 
  arrange(sym_cd, fs_q) %>% 
  collect() %>% 
  mutate(fs_q = as.yearqtr(fs_q))
# 
# 
# df_new_prep <- # 기존 종목 전처리 테이블_최종
#   preprocess_fs_pl(df_new)  
# 
# db_upsert('company_fs_pl_prep',
#           df_new_prep, 
#           c('sym_cd', 'fs_q'))
