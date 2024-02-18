source("utils_db.R")
library(tidyverse)
library(timetk)

# 밸류팩터 얻기
get_value_factors <- function(.sym=NULL, .start=NULL, .end=NULL){
  print(.start)
  filled_fs_bs <-
    db_obj('company_fs_bs') %>% 
    expand(sym_cd, fs_q) %>% 
    left_join(
      db_obj('company_fs_bs'),
      by=c('sym_cd','fs_q')
    ) %>% 
    group_by(sym_cd) %>% 
    dbplyr::window_order(fs_q) %>% 
    fill(grs_asset:net_asset)
  
  db_obj('stock_daily') %>%  
    select(num_key:sym_cd, mkt_cap:num_stock_t) %>%
    left_join(
      db_obj('stock_cons') %>% 
        select(num_key:bps_12mf),
      by='num_key'
    )  %>% 
    mutate(earn_12mf = ifelse(is.na(earn_dom_12mf),
                              earn_12mf,
                              earn_dom_12mf)
    )%>% 
    left_join(
      db_obj('workdays') %>% 
        select(-fs_y),
      by= 'base_dt'
    ) %>%
    left_join(
      filled_fs_bs %>% 
        select(-grs_asset:-cap),
      by=c('sym_cd','fs_q')
    ) %>%
    left_join(
      db_obj('company_fs_pl_prep') %>% 
        select(sym_cd:cash_flow_a4q),
      by=c('sym_cd','fs_q')
    ) %>% 
    sym_dt_pick(.sym, .start, .end) |> 
    collect() |> 
    mutate(adj_mkt_cap = mkt_cap / num_stock_o * num_stock_t ) %>% 
    transmute(
      num_key = num_key,
      # base_dt  = base_dt,
      # sym_cd   = sym_cd,
      inv_per  = earn_a4q          / adj_mkt_cap,
      inv_por  = opr_prof_a4q      / adj_mkt_cap,
      inv_psr  = sales_a4q         / adj_mkt_cap,
      inv_pocr = op_cash_flow_a4q  / adj_mkt_cap,
      inv_pcr  = cash_flow_a4q     / adj_mkt_cap,
      inv_pbr  = net_asset         / adj_mkt_cap,
      inv_per_12mf = earn_12mf     / adj_mkt_cap,
      inv_por_12mf = opr_prof_12mf / adj_mkt_cap,
      inv_psr_12mf = sales_12mf    / adj_mkt_cap,
      inv_pbr_12mf = bps_12mf      / (mkt_cap / num_stock_o)
    )
}

update_factors <- function(.start, .end){

  BASE_DT <- db_obj('workdays') %>%
    sym_dt_pick(.start=.start, .end=.end) %>%
    pull(base_dt)

  df <- map(BASE_DT,
      ~db_upsert('factor_daily',
                 get_value_factors(.start=.x, .end=.x),
                 'num_key'))
}

update_factors('2022-12-01', '2023-06-30')

####################################################################

roll_3m_ret <- slidify(prod, 60, 'right')
roll_6m_ret <- slidify(prod, 120, 'right')
roll_12m_ret <- slidify(prod, 252, 'right')

fcn <- function(sym){
  print(paste(sym, "크롤링..."))
  df <- db_obj('stock_daily') %>% distinct(base_dt) %>% collect() %>%
    left_join(
      db_obj('stock_daily') %>%
        filter(sym_cd==sym) %>%
        transmute(num_key = num_key,
                  base_dt = base_dt,
                  gret = close/base_p) %>%
        collect(),
      by='base_dt'
    )  %>%
    transmute(
      num_key = num_key,
      ret_3m=roll_3m_ret(gret)-1,
      ret_6m=roll_6m_ret(gret)-1,
      ret_12m=roll_12m_ret(gret)-1
    )

  df2 <- db_obj('stock_daily') %>%
    filter(sym_cd==sym) %>%
    transmute(num_key) %>%
    collect() %>%
    left_join(df, by='num_key')

  print("완료!")
  return(df2)
}

all_sym_list <- db_obj('stock_daily') %>% distinct(sym_cd) %>% pull()
sym_list <- all_sym_list %>% .[3013:3044]

map(sym_list,
    ~db_upsert(
      'factor_daily',
      fcn(.x),
      'num_key'))



df <- factor_sym_dt('005930', '2020-01-01') %>%
  select(base_dt, ret_12m) %>%
  collect()

for (i in all_sym_list[3010:3020]){

  factor_sym_dt(all_sym_list[3010]) %>% select(sym_cd,base_dt,ret_12m) %>%
    summarise(
      sym_cd = sym_cd[1],
      count = sum(!is.na(ret_12m))
    ) %>%
    collect()

}

df <- map_dfr(all_sym_list[2900:3000], ~factor_sym_dt(.x) %>% select(sym_cd,base_dt,ret_12m) %>%
  summarise(
    sym_cd = sym_cd[1],
    count = sum(!is.na(ret_12m))
  ) %>%
  collect())
