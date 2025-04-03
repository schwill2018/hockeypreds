# Install taskscheduleR if not already installed:
# install.packages("taskscheduleR")

library(taskscheduleR)
setwd("C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main")
files_path <- getwd()
# Define the full path to your R script that pulls the odds.
# For example, if your script is saved as "pull_nhl_odds.R" in "C:/MyProjects/OddsApp"
rscript <- paste0(files_path, "/team_get_odds.R")

# Set the start time to 11:00 AM
start_time <- "10:00"
# Set the start date to tomorrow's date in mm/dd/yyyy format
start_date <- format(Sys.Date() + 1, "%m/%d/%Y")

# Create a scheduled task that runs every 12 hours starting tomorrow at 11 AM
taskscheduler_create(
  taskname   = "Pull_NHL_Odds_Task", 
  rscript    = rscript,
  schedule   = "HOURLY",    # Base schedule: hourly
  modifier   = 12,          # Modifier: every 12 hours
  starttime  = start_time,  
  startdate  = start_date   
)

taskscheduler_runnow("Pull_NHL_Odds_Task")
# You can check the scheduled task by running:
taskscheduler_ls("Pull_NHL_Odds_Task")
# getwd()
# #if you need to stop task
# taskscheduler_stop("Pull_NHL_Odds_Task")
# taskscheduler_delete("Pull_NHL_Odds_Task")
