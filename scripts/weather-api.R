####################################################################
########################### script setup ########################### 
####################################################################

### create sink connections to capture errors in log files
sink_message <- file("~/open-weather-map/execution-message.Rout", open = "wt")
sink(sink_message, type = "message", append = FALSE)

sink_output <- file("~/open-weather-map/execution-output.Rout", open = "wt")
sink(sink_output, type = "output", append = FALSE)

### specify working directory
wd <- '~/open-weather-map'

### set working directory
setwd(wd)

### specify packages required for the script
### currently, the Pacman package is unavailable for this version of RStudio Server
### So it is manually replicated below
package_list <- c("dplyr",
                  "stringr",
                  "rjson",
                  "lubridate",
                  "tidyr",
                  "jsonlite",
                  "RCurl",
                  "RMySQL",
                  "readr")

### find packages that are required that are not installed
install_packages <- package_list[!(package_list %in% installed.packages()[,"Package"])]

### if there is something to install, do it
if(length(install_packages) > 0) {install.packages(install_packages)}

### library the (now) installed packages
sapply(package_list,
       require,
       character = TRUE)

### encode user name for Redshift (these need to be stored in .bash_profile and/or .Renviron file)
open_weather_api_key <- Sys.getenv("open_weather_api_key")

### get user name (these need to be stored in .bash_profile and/or .Renviron file)
mysql_user_name <- Sys.getenv("mysql_user_name")

### get password (these need to be stored in .bash_profile and/or .Renviron file)
mysql_password <- Sys.getenv("mysql_password")

### get host (these need to be stored in .bash_profile and/or .Renviron file)
mysql_host <- Sys.getenv("mysql_host")

### connect to MySQL database
database_connection <- RMySQL::dbConnect(
  RMySQL::MySQL(),
  dbname = 'barutt_prod',
  username = mysql_user_name,
  password = mysql_password,
  host = mysql_host,
  port = 3306,
)

### remove objects
rm(mysql_user_name)
rm(mysql_password)
rm(mysql_host)
rm(package_list)
rm(install_packages)

####################################################################
######################## get data from API ######################### 
####################################################################

### specify base URL
base_url <- 'https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=latitude&lon=longitude&dt=datetime&appid=apikey&units=imperial'

### city lookup
city_lookup <- read.csv(file = '~/open-weather-map/city-lookup.csv', header = TRUE, stringsAsFactors = FALSE)
colnames(city_lookup)[1] <- 'city'
colnames(city_lookup)[2] <- 'latitude'
colnames(city_lookup)[3] <- 'longitude'

### get vector of unique cities
unique_cities <- unique(city_lookup$city)

### specify list to store output
city_output <- list()

### for each city, specify url; get and cleanse data; append to list
for (i in 1:length(unique_cities)){
  
  ### specify url for specific city
  specific_url <- gsub(x = 
                  gsub(x = 
                  gsub(x = 
                  gsub(x = base_url, 
                       pattern = 'latitude', 
                       replacement = city_lookup$latitude[i]),
                       pattern = 'longitude',
                       replacement = city_lookup$longitude[i]),
                       pattern = 'datetime',
                       replacement = round(as.numeric(Sys.time()),0) - (86400*1)),
                       pattern = 'apikey',
                       replacement = open_weather_api_key)
  
  ### get data from API
  tmp_df <- fromJSON(getURL(specific_url))
  
  ### get hourly dataframe
  tmp_hourly <- tmp_df$hourly
  
  ### add columns if they do not exist; otherwise, handle dataframes included in the dataframe
  if ("snow" %in% colnames(tmp_hourly) == FALSE) {tmp_hourly <- tmp_hourly %>% mutate(snow = 0)} else {tmp_hourly$snow <- coalesce(tmp_hourly$snow$`1h`,0)}
  if ("rain" %in% colnames(tmp_hourly) == FALSE) {tmp_hourly <- tmp_hourly %>% mutate(rain = 0)} else {tmp_hourly$rain <- coalesce(tmp_hourly$rain$`1h`,0)}
  
  ### cleanse output
  tmp_hourly <- tmp_hourly %>%
                mutate(dt = as.POSIXct(as.numeric(dt), origin = '1970-01-01'),
                       city = city_lookup$city[i]) %>%
                select(city,
                       dt,
                       temp,
                       feels_like,
                       pressure,
                       humidity,
                       dew_point,
                       uvi,
                       clouds,
                       visibility,
                       wind_speed,
                       wind_deg,
                       wind_gust,
                       rain,
                       snow)
  
  ### append to list
  city_output[[length(city_output) + 1]] <- tmp_hourly
  
  ### print progress
  print(i)
  
}

### append to dataframe
output_data <- do.call(rbind, city_output)

####################################################################
######################## get data from API ######################### 
####################################################################

### write data do MySQL database
dbWriteTable(conn = database_connection,
             name = 'daily_upload',
             value = output_data,
             append = TRUE,
             row.names = FALSE)

### remove duplicates
dbSendQuery(conn = database_connection, read_file(file = 'remove-duplicates.sql'))

### confirm runtime
write.table(x = Sys.time(), file = 'runtime.txt')

### close sink connection (message)
sink(type = "message")
close(sink_message)

### close sink connection (output)
sink(type = "output")
close(sink_output)