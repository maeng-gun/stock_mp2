library(shiny)
library(bs4Dash)
library(shinyWidgets)
library(waiter)
library(DT)
library(glue)
library(httr)
source('utils_db.R')



ui <- dashboardPage(
  dashboardHeader(),
  dashboardSidebar(),
  dashboardBody(
    
    ################ 여기서부터 ui 코딩 ####################
    
    useSweetAlert(),
    box(
      title = "데이터수집 자동화 선택",
      status = "lightblue",
      solidHeader = T,
      width = 12,
      my_buttons_UI(
        id = 'crawling',
        btn_names = c('수집','초기화')
      ),
      br(),
      fluidRow(
        column(
          width=4,
          uiOutput('years_2b_updated')
        ),
        column(
          width=4,
          uiOutput('days_2b_updated')
        ),
        column(
          width=4,
          uiOutput('quarters_2b_updated')
        )
      ),
      fluidRow(
        column(
          width=8,
          checkboxGroupInput(
            inputId = 'check',
            label = '수집 자동화 절차 선택',
            choices = c("영업일 정보" = "workdays", 
                        "주가지수 목록" = "index_list",
                        "일별 주식정보" = "daily",
                        "분기 재무정보" = "quarterly"),
            selected = c(
              "workdays","index_list", "daily","quarterly"
            ),
            inline = T
          )
        ),
        column(
          width=4,
          br(),
          prettySwitch(
            inputId = "save",
            label = "크롤링 결과 저장하기", 
            status = "success",
            value = T,
            fill = TRUE
          )
        )
      ),
      uiOutput('results')
    )
    ################ 여기까지 ui 코딩 ####################
  )
)

server <- function(input, output, session) {
  
  ################ 여기부터 server 코딩 ####################

  library(tidyverse)
  
  values <- reactiveValues(
    visual=F, 
    check=NULL,
    old_stocks_list = NULL,
    new_stocks_list = NULL,
    years_2b_updated = years_2b_updated(),
    days_2b_updated = days_2b_updated(),
    quarters_2b_updated = quarters_2b_updated()
  )

  output$years_2b_updated <- renderUI({
    
    selectInput(
      inputId = 'years_2b_updated',
      label = '영업일 업데이트 대상 연도',
      choice = values$years_2b_updated,
      selected = values$years_2b_updated,
      multiple = T
    )
  })
    
  output$days_2b_updated <- renderUI({
    
    pickerInput(
      inputId = 'days_2b_updated',
      label = '일별 업데이트 대상 일자',
      choices = values$days_2b_updated,
      selected = values$days_2b_updated,
      multiple = T,
      options = list(
        `actions-box` = TRUE,
        `select-all-text` = "전체선택",
        `deselect-all-text` = "전체해제"
      )
    )
  })
  
  output$quarters_2b_updated <- renderUI({
    
    selectInput(
      inputId = 'quarters_2b_updated',
      label = '재무지표 업데이트 대상 분기',
      choices = values$quarters_2b_updated,
      selected = values$quarters_2b_updated,
      multiple = T
    )
  })
  
  select_list <- list("workdays" = "영업일 정보", 
                      "index_list" = "주가지수 목록",
                      'stock_info' = "종목 정보",
                      'stock_daily' = "일별 주가",
                      'stock_cons' = "일별 컨센서스",
                      'stock_managed' = "관리종목 정보",
                      'stocks_in_index' = "지수구성종목",
                      'index_daily' = "일별 지수정보",
                      'old_company_fs_bs' = "기존종목 BS 지표",
                      'old_company_fs_pl' = "기존종목 PL 지표",
                      'old_company_fs_pl_prep' = "기존종목 PL 전처리",
                      'new_company_fs_bs' = "신규종목 BS 지표",
                      'new_company_fs_pl' = "신규종목 PL 지표",
                      'new_company_fs_pl_prep' = "신규종목 PL 전처리"
                      )
  
  observeEvent(input$`crawling-btn1`,{
    ask_confirmation(
      inputId = "confirm1",
      title = "DataGuide는 실행하셨나요?",
      text = "- 실행했다면 계속하기 -",
      btn_labels = c("취소", "계속")
    )
  })
    
  observeEvent(input$confirm1,{
    
    if(isTRUE(input$confirm1)){
      values$check <- input$check
      values$old_stocks_list = old_stocks_list()
      values$years_2b_updated = input$years_2b_updated
      values$days_2b_updated = input$days_2b_updated
      values$quarters_2b_updated = input$quarters_2b_updated
      
      if('workdays' %in% values$check){
        res <- map_dfr(values$years_2b_updated, 
                       ~get_workdays_from_krx(.x, save=input$save))
        output$workdays <- renderDT(res, options = list(scrollX = TRUE))
      }
      if('index_list' %in% values$check){
        res <- get_index_list(save=input$save)
        output$index_list <- renderDT(res, options = list(scrollX = TRUE))
      }
      if('daily' %in% values$check){
        day_list <- gsub("-","",values$days_2b_updated)
        total_bar <- length(day_list)*7
        progressSweetAlert(
          id = "myprogress",
          title = "일별 주식데이터 수집 중...",
          display_pct = TRUE, value = 0, total = total_bar
        )
        
        source('utils_crawling.R')  
        
        res_daily <- imap(
          day_list,
          ~get_daily_tables(.x, .y, total_bar, save=input$save)) %>%
          pmap(bind_rows)
        
        closeSweetAlert()
        
        daily_table_names <- c('stock_info', 'stock_daily', 'stock_cons',
                               'stock_managed', 'stocks_in_index',
                               'index_daily')
        
        
        map(daily_table_names, function(i){
          output[[i]] <<- renderDT(
            res_daily[[i]],
            options = list(scrollX = TRUE)
          )
        })
      }
      
      if('quarterly' %in% values$check){
        
        values$new_stocks_list = 
          new_stocks_list(values$old_stocks_list)
        
        if(nrow(values$new_stocks_list)==0){
          total_value <- 3
        } else {
          total_value <- 5
        }
        
        progressSweetAlert(
          id = "myprogress2",
          title = "분기 재무데이터 수집 중...",
          display_pct = TRUE, value = 0, total = total_value
        )
        
        source('utils_crawling.R')  
        
        updateProgressBar(id = "myprogress2", value = 1, total=total_value)
        
        res_q_old <- map(values$quarters_2b_updated,
                         ~get_company_fs_tables.old_sym(
                           .x, values$old_stocks_list, save=input$save)
        ) %>% pmap(bind_rows)
        
        old_table_names <- c('old_company_fs_bs', 'old_company_fs_pl')
        
        map(old_table_names, function(i){
          output[[i]] <<- renderDT(
            res_q_old[[i]],
            options = list(scrollX = TRUE)
          )
        })
        
        updateProgressBar(id = "myprogress2", value = 2, total=total_value)
        
        res_q_prep_old <- df_old_prep(
          values$quarters_2b_updated, save=input$save)
        
        output$old_company_fs_pl_prep <- renderDT(
          res_q_prep_old, options = list(scrollX = TRUE)
        )
        updateProgressBar(id = "myprogress2", value = 3, total=total_value)
        
        if(nrow(values$new_stocks_list)>0){
          res_q_new <- 
            get_company_fs_tables.new_sym(values$new_stocks_list, save=input$save)
          
          new_table_names <- c('new_company_fs_bs', 'new_company_fs_pl')
          
          map(new_table_names, function(i){
            output[[i]] <<- renderDT(
              res_q_new[[i]],
              options = list(scrollX = TRUE)
            )
          })
          
          updateProgressBar(id = "myprogress2", value = 4, total=total_value)
          
          res_q_prep_new <- df_new_prep(
            values$new_stocks_list, save=input$save)
          
          output$new_company_fs_pl_prep <- renderDT(
            res_q_prep_new, options = list(scrollX = TRUE)
          )
          
          updateProgressBar(id = "myprogress2", value = 5, total=total_value)
        } else {
          
        }
        
        closeSweetAlert()
      }
      values$visual <- T
    } else {
      
    }
  }, ignoreNULL = TRUE)
  
  observeEvent(input$`crawling-btn2`,{
    values$visual <- F
    updateSelectInput(session,
                      inputId = 'years_2b_updated',
                      choices = years_2b_updated(),
                      selected = years_2b_updated())
    updatePickerInput(session,
                      inputId = 'days_2b_updated',
                      choices = days_2b_updated(),
                      selected = days_2b_updated())
    updateSelectInput(session,
                      inputId = 'quarters_2b_updated',
                      choices = quarters_2b_updated(),
                      selected = quarters_2b_updated())
  })
  
  output$results <- renderUI({
    ns <- session$ns
    if(values$visual){
      
      render_list <- c()
      
      if("workdays"%in% values$check){
        render_list <- c('workdays')
      }
      if("index_list"%in% values$check){
        render_list <- c(render_list, 'index_list')
      }
      if('daily' %in% values$check){
        render_list <- c(render_list, 
                         'stock_info',
                         'stock_daily',
                         'stock_cons',
                         'stock_managed',
                         'stocks_in_index',
                         'index_daily')
      }
      if("quarterly" %in% values$check){
        render_list <- c(render_list, 
                         "old_company_fs_bs",
                         "old_company_fs_pl",
                         "old_company_fs_pl_prep")
        
        if(nrow(values$new_stocks_list)>0){
          render_list <- c(render_list, 
                           'new_company_fs_bs', 
                           'new_company_fs_pl',
                           'new_company_fs_pl_prep')
        }
      }
      
      menu_tab <- map(render_list, {
        ~tabPanel(
          title = select_list[.x],
          DTOutput(.x)
        )
      })
      
      tabBox(
        id = "mytabbox",
        width = 12,
        status = "primary",
        type = "tabs",
        .list = menu_tab
      )
    }else{
      
    }
  })
  
  ################ 여기까지 server 코딩 ####################
}

shinyApp(ui, server)
