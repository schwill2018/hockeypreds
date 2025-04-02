library(shiny)
library(shinydashboard)
library(tidyverse)
library(DT)

# UI
ui <- dashboardPage(
  dashboardHeader(title = "Team/Player Dashboard"),
  dashboardSidebar(
    selectInput("view_type", "View By:", choices = c("Team", "Player"), selected = "Team"),
    conditionalPanel(
      condition = "input.view_type == 'Team'",
      selectInput("team_select", "Select Team:", choices = c("All", NULL), selected = "All")
    ),
    conditionalPanel(
      condition = "input.view_type == 'Player'",
      selectInput("player_team_select", "Select Team:", choices = c("All", NULL), selected = "All"),
      selectInput("player_select", "Select Player:", choices = c("All", NULL), selected = "All")
    ),
    conditionalPanel(
      condition = "input.view_type == 'Player'",
      selectInput("player_metric", "Select Prediction Type:", choices = c("All", "earned_point", "earned_goal", "earned_assist"), selected = "All")
    ),
    conditionalPanel(
      condition = "input.view_type == 'Team'",
      selectInput("team_metric", "Select Prediction Type:", choices = c("All", "game_won", "game_won_spread"), selected = "All")
    )
  ),
  dashboardBody(
    tabsetPanel(
      tabPanel("Interactive Data",
               fluidRow(
                 box(DTOutput("data_table"), width = 12)
               ),
               fluidRow(
                 box(selectInput("x_var", "X-axis:", choices = NULL), width = 6),
                 box(selectInput("y_var", "Y-axis:", choices = NULL), width = 6)
               ),
               fluidRow(
                 box(plotOutput("scatter_plot"), width = 12)
               )
      ),
      tabPanel("Predictions",
               fluidRow(
                 box(DTOutput("pred_table"), width = 12)
               )
      ),
      tabPanel("Performance",
               fluidRow(
                 box(title = "Model vs Bookmaker Performance", DTOutput("perf_table"), width = 6),
                 box(title = "Performance Trend", plotOutput("perf_plot"), width = 6)
               )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  
  # Dummy placeholder datasets
  team_data <- reactive({
    tibble(team = c("A", "B"), x = rnorm(2), y = rnorm(2), team_id = c("A", "B"))
  })
  player_data <- reactive({
    tibble(player = c("P1", "P2"), team_id = c("A", "B"), x = rnorm(2), y = rnorm(2))
  })
  prediction_data <- reactive({
    tibble(game_id = 1:2, prediction = c(0.6, 0.4), some_info = c("info1", "info2"))
  })
  bookmaker_data <- reactive({
    tibble(game_id = 1:2, bookmaker_1 = c(0.55, 0.45), bookmaker_2 = c(0.57, 0.43))
  })
  performance_data <- reactive({
    tibble(game_date = as.Date('2023-01-01') + 0:1,
           model_perf = c(0.65, 0.67),
           bookmaker_perf = c(0.6, 0.62))
  })
  
  # Update selectors for team and player
  observe({
    updateSelectInput(session, "team_select", choices = c("All", unique(team_data()$team)))
    updateSelectInput(session, "player_team_select", choices = c("All", unique(player_data()$team_id)))
  })
  
  observeEvent(input$player_team_select, {
    if (input$player_team_select == "All") {
      updateSelectInput(session, "player_select", choices = c("All", unique(player_data()$player)))
    } else {
      updateSelectInput(session, "player_select",
                        choices = c("All", player_data() %>% filter(team_id == input$player_team_select) %>% pull(player)))
    }
  })
  
  # Data filtering for Interactive Tab
  selected_data <- reactive({
    if (input$view_type == "Team") {
      if (input$team_select == "All") return(team_data())
      filter(team_data(), team == input$team_select)
    } else {
      if (input$player_select == "All") return(player_data())
      filter(player_data(), player == input$player_select)
    }
  })
  
  observe({
    updateSelectInput(session, "x_var", choices = names(selected_data()))
    updateSelectInput(session, "y_var", choices = names(selected_data()))
  })
  
  output$data_table <- renderDT({
    datatable(selected_data())
  })
  
  output$scatter_plot <- renderPlot({
    req(input$x_var, input$y_var)
    ggplot(selected_data(), aes_string(x = input$x_var, y = input$y_var, color = "team_id")) +
      geom_point() +
      theme_minimal()
  })
  
  # Predictions Tab
  output$pred_table <- renderDT({
    req(input$view_type)
    preds <- merged_preds()
    preds$avg_bookmaker_prob <- rowMeans(select(preds, contains("bookmaker_")), na.rm = TRUE)
    # Placeholder: Add filtering based on team_metric or player_metric only if not "All"
    datatable(preds, options = list(pageLength = 5))
  })
  
  # Performance Tab
  output$perf_table <- renderDT({
    req(input$view_type)
    # Placeholder: Add filtering based on team_metric or player_metric only if not "All"
    datatable(performance_data())
  })
  
  output$perf_plot <- renderPlot({
    req(input$view_type)
    ggplot(performance_data(), aes(x = game_date)) +
      geom_line(aes(y = model_perf, color = "Model")) +
      geom_line(aes(y = bookmaker_perf, color = "Bookmakers")) +
      theme_minimal() +
      labs(y = "Performance", color = "Source")
  })
  
  # Join predictions and bookmaker data
  merged_preds <- reactive({
    left_join(prediction_data(), bookmaker_data(), by = c("game_id"))
  })
}

# Run App
shinyApp(ui, server)
