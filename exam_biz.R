library(zoo)
library(tidyquant)

FANG %>% filter(symbol=="FB") %>% 
  tq_mutate(select=date,
            mutate_fun=as.yearqtr)

FANG %>% mutate(date=yearquarter(date))

str(FANG)

as_tibble(df) %>% filter(sym_cd=='005930') %>% 
  tq_transmute(select=close,
            mutate_fun=lag.xts,
            k=1) 

df <- 
  db_obj('stock_daily') %>% 
  filter(between(base_dt, "2022-10-01","2022-10-31")) %>% 
  collect() %>% 
  as_tsibble(key=sym_cd, 
             index=base_dt)

df=db_obj('company_fs_bs') %>% 
  collect()
df1 <- df %>% transmute(
  yq = as.numeric(make_yearquarter(year,quarter))
)
df %>% mutate(
  yq = make_yearquarter(year,quarter),.before = 2
) %>% .$yq

yearquarter(0)


year
db_obj('workdays') %>% filter(year(base_dt)==2022)

get_workdays_from_krx(2022)


  
year=2024

seq_along()


workdays <- 
  db_obj('workdays') %>% 
  select('base_dt') %>% 
  collect() %>% .$base_dt

workdays

a <- bizseq(first(holidays),last(holidays),"actual")

workdays[year(workdays)==2022]


DBI::dbCreateTable()

dbDataType(con, )

