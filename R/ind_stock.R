library(plotly)


#[함수] 종목명 벡터를 종목코드 벡터로 변경
get_sym_cd <- function(...){
  symbols <- c(...)
  db_obj('stock_info') |> 
    arrange(desc(info_update)) |> 
    distinct(sym_cd, .keep_all = T) |> 
    filter(sym_nm %in% symbols) |> 
    collect() |> 
    pull(sym_cd) |> 
    suppressWarnings()
}

#[함수] 선택된 종목의 시/고/저/종가(원가격) 얻기
get_stock_ohlc <- function(...){
  
  sym_codes <- get_sym_cd(...)
  
  db_obj('stock_daily') |>
    select(sym_cd,base_dt,open:base_p) |> 
    filter(sym_cd %in% sym_codes) |> 
    arrange(sym_cd,base_dt) |> 
    collect()
}

#함수 : 종목 olhc -> 수정종가
get_adj_olhc <- function(olhc, close_only=F){
  
  df <- olhc |> 
    group_by(sym_cd) |> 
    mutate(
      close_adj = ((base_p / close) |> rev() |> 
                     cumprod() |> rev() |> 
                     lead(1, default=1)) * last(close))
  if(close_only){
    df |> transmute(sym_cd, base_dt, close=close_adj) |> 
      ungroup()
  } else {
    df |> transmute(
      sym_cd, base_dt,
      across(open:close, ~round((.x/close*close_adj)))
    )|> 
      ungroup()
  }
}



add_rangeselector <- function(fig){
  fig |>
    layout(
      xaxis = list(
        rangeselector=list(
          buttons=list(
            list(label="1m", count=1, step="month", stepmode="backward"),
            list(label="6m", count=6, step="month", stepmode="backward"),
            list(label="1y", count=1, step="year", stepmode="backward"),
            list(label="MTD", count=1, step="month", stepmode="todate"),
            list(label="YTD",count=1, step="year", stepmode="todate"),
            list(label="3y", count=3, step="year", stepmode="backward"),
            list(label="5y", count=5, step="year", stepmode="backward"),
            list(label="10y", count=10, step="year", stepmode="backward"),
            list(step="all")))))
}

add_xy_style <- function(fig){
  fig |>
    layout(
      xaxis = list(
        zerolinecolor = '#ffff',
        zerolinewidth = 2,
        gridcolor = 'ffff'),
      yaxis = list(
        exponentformat='none',
        zerolinecolor = '#ffff',
        zerolinewidth = 2,
        gridcolor = 'ffff'),
      plot_bgcolor='#e5ecf6')
  
}

stock_price_line_plot <- function(data){
  data |> 
    plot_ly(type = 'scatter', mode = 'lines') |> 
    add_trace(x = ~base_dt, y = ~close) |> 
    layout(showlegend = F,
           xaxis = list(title=''),
           yaxis = list(title='수정종가')) |> 
    add_xy_style() |> 
    add_rangeselector()
}

stock_price_cdl_plot <- function(data){
  data |> 
    plot_ly(
      type = "candlestick",
      x = ~base_dt, 
      open  = ~open, 
      high  = ~high, 
      low   = ~low,
      close = ~close,
      increasing = list(line = list(color = '#FF0000')), 
      decreasing = list(line = list(color = '#0000FF'))) |> 
    layout(showlegend = F,
           xaxis = list(title='',
                        rangeslider = list(visible = F)),
           yaxis = list(title='수정종가')) |> 
    add_xy_style() |> 
    add_rangeselector()
}




ind_stock_UI <- function(id) {
  ns <- NS(id)
  tagList(
    #본문1 - 1 : 입력박스
    box(
      title = "개별주식 정보 입력",
      status = "info",
      solidHeader = T,
      width = 12,
      
      fluidRow(
        column(8),
        column(2,
               actionButton(ns('btn1'),"초기화", width='100%'),
        ),
        column(2,
               actionButton(ns('btn2'),"조회", width='100%'),
        )
      ),
      br(),
      fluidRow(
        column(
          width = 4,
          textInput(
            inputId = ns("sym_nm"),
            label = "종목명",
            placeholder = "ex) 삼성전자"
          )
        ),
        column(
          width = 4,
          dateRangeInput(
            inputId = ns("date_range"),
            label = "기간",
            start = "2005-01-01",
            end = "2022-12-31"
          )
        ),
        column(
          width = 4,
          selectInput(
            inputId = ns("visual"),
            label = "시각화 종류",
            choices = c(선차트 = "line_plot", 캔들차트 = "cdl_plot",
                        테이블 = "tbl")
          )
        )
      )
    ),
    uiOutput(outputId = ns("ind_stock_info"))
  )
}

ind_stock_Server <- function(id) {
  moduleServer(
    id,
    function(input, output, session) {
      
      #입력된 종목명의 ohlc테이블
      values <- reactiveValues(visual = '')
      
      observeEvent(input$btn2,{
        values$sym_nm <- input$sym_nm
        values$start <- input$date_range[1]
        values$end <- input$date_range[2]
        values$visual <- input$visual
      })
      
      observeEvent(input$btn1, {
        updateTextInput(session, 'sym_nm', value = '')
        updateDateRangeInput(session, 'date_range',
                             start = '2005-01-01',
                             end = '2022-12-31')
        updateSelectInput(session, 'visual', selected = 'tbl')
        values$visual <- ''
      })
      
      ohlcInput1 <- reactive({
        get_stock_ohlc(values$sym_nm)
      })
      
      ohlcInput2 <- reactive({
        ohlcInput1() |>
          timetk::filter_by_time(base_dt,values$start, values$end)
      })
      
      output$tbl <- renderDT({
        ohlcInput2() |>
          get_adj_olhc() |>
          select(-sym_cd) |>
          setNames(c('일자','시가','저가','고가','종가'))
      })
      
      output$line_plot <- renderPlotly(
        ohlcInput2() |>
          get_adj_olhc(close_only=T) |>
          stock_price_line_plot()
      )
      
      output$cdl_plot <- renderPlotly(
        ohlcInput2() |>
          get_adj_olhc() |>
          stock_price_cdl_plot()
      )
      
      output$ind_stock_info <- renderUI({
        
        ns <- session$ns
        
        if(values$visual=="tbl"){
          DTOutput(ns("tbl"))
        } else if(values$visual=="line_plot"){
          plotlyOutput(ns("line_plot"))
        } else if(values$visual=="cdl_plot"){
          plotlyOutput(ns("cdl_plot"))
        } else {
          
        }
      })
      
    }
  )
}