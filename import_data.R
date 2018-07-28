# Data ETL Functions
library(dplyr)
library(lubridate)

import.market.data <- function(securities.path, ratings.path, stocks.path, sca.path) {
  
  securities.raw <- read.csv(securities.path, sep = ',')
  securities.df <- etl.securities(securities.raw)
  
  ratings.raw <- read.csv(ratings.path, sep = ',')
  ratings.df <- etl.ratings(ratings.raw)
  
  stocks.raw <- read.csv(stocks.path, sep = '\t', header = T, fileEncoding = 'UTF-16LE')
  stocks.df <- etl.stocks(stocks.raw)
  
  sca.raw <- read.csv(sca.path, sep = ',')
  sca.df <- etl.sca(sca.raw, securities.df$tic)
  
  # Combine and transform as necessary the four data sources.
  # (SEC data source will be added after, it does not depened on external calculation)
  
  
  securities.w.ratings <- securities.df %>% full_join(ratings.df, by=c('tic', 'year'))
  # only 41% of the companies are rated. Maybe better off as rated_good/rated_bad/unrated
  
  df.with.stocks.added <- securities.w.ratings %>% full_join(stocks.df, by=c('tic', 'year'))
  unified.df <- df.with.stocks.added %>% full_join(sca.df, by=c('tic', 'year'))
  
  # NOTE:  We may want to start with another dataframe first. Possibly the fundamentals when we have it.
  
  # remember to represent the litigation dollars as percent of company value.
  
  # bring in fundamentals and wittle down the columns.
  # analysis can begin to create normality!!!
  return(unified.df)
}

etl.securities <- function(securities.df) {
  securities.df <- transform.null.values.securities(securities.df)
  trimmed.df <- data.frame(
    "tic" =          as.character(securities.df$tic),
    "year" =         year(as.Date(securities.df$datadate, format = "%m/%d/%y")),
    "trade.volume" = securities.df$cshtrm,
    "price.close" =  securities.df$prccm,
    stringsAsFactors=FALSE)

  summary.df <- trimmed.df %>% 
    group_by(tic, year) %>% 
    summarize_at(c('trade.volume', 'price.close'), mean)
  
  summary.w.trends.df <- calc.trends(summary.df, securities.df)
  return(summary.w.trends.df)
}

transform.null.values.securities <- function(securities.df) {
  securities.df$cshtrm[is.na(securities.df$cshtrm)] <- 0
  securities.df <- subset(securities.df, !is.na(prccm))
  return(securities.df)
}

calc.trends <- function(yearly.df, monthly.df) {
  yearly.df[, 'price.trend'] <- NA
  for(row in 1:nrow(yearly.df)) {
    # for this tic and year, subset the monthly.df on the same tic and year.
    # then sort by date and get price close[last] - price close[first]
    company <- yearly.df$tic[row]
    year <- yearly.df$year[row]
    temp.df <- subset(monthly.df, as.character(tic) == company & year(as.Date(datadate, format = "%m/%d/%y")) == year)
    ordered.temp.df <- temp.df[order(as.Date(temp.df$datadate, format = "%m/%d/%y")),]
    closing.price.trend <- tail(ordered.temp.df$prccm, 1) - head(ordered.temp.df$prccm, 1)
    yearly.df$price.trend[row] <- closing.price.trend
  }
  return(yearly.df)
}

etl.ratings <- function(ratings.df) {
  trimmed.ratings.df <- data.frame(
    'tic' =            as.character(ratings.df$tic),
    'year' =           year(as.Date(ratings.df$datadate, format = "%m/%d/%y")),
    'company.rating' = ifelse(
                         as.character(ratings.df$spcsrc) != ' ',
                         as.character(ratings.df$spcsrc),
                         as.character(ratings.df$splticrm)),
    stringsAsFactors=FALSE)
  
  agg.ratings.df <- subset(trimmed.ratings.df, company.rating != ' ')
  agg.ratings.df$company.rating <- determine.rating.score(agg.ratings.df$company.rating)
  
  yearly.ratings.df <- agg.ratings.df %>%
    group_by(tic, year) %>%
    summarize_at('company.rating', mean)
    
  return(yearly.ratings.df)
}

determine.rating.score <- function(ratings) {
  scores <- c()
  for(rating in ratings) {
    score <- 0
    if(startsWith(rating, 'A')) {
      score <- 4
    }
    if(startsWith(rating, 'B')) {
      score <- 3
    }
    if(startsWith(rating, 'C')) {
      score <- 2
    }
    if(startsWith(rating, 'D')) {
      score <- 1
    }
    scores <- c(scores, score)
  }
  return(scores)
}

etl.stocks <- function(stocks.df) {
  trimmed.df <- data.frame(
    "tic" =                as.character(stocks.df$tic),
    "year" =               year(as.Date(stocks.df$datadate, format = "%m/%d/%Y")),
    "eps" =                stocks.df$eps,
    "shares.outstanding" = stocks.df$cshoc,
    stringsAsFactors=FALSE)
  
  yearly.df <- trimmed.df %>%
    group_by(tic, year) %>%
    summarize_at(c('eps', 'shares.outstanding'), mean)
  
  yearly.df <- subset(yearly.df, !is.na(eps) & !is.na(shares.outstanding))
  return(yearly.df)
}

etl.sca <- function(sca.df, companies) {
  sca.df$Ticker <- as.character(sca.df$Ticker)
  filtered.df <- sca.df[sca.df$Ticker %in% companies,]
  trimmed.df <- data.frame(
    "tic" = as.character(filtered.df$Ticker),
    "year" = filtered.df$FilingYear,
    "was.dismissed" = toupper(as.character(filtered.df$Dismissed)),
    "settlement" = filtered.df$SettlementAmount,
    stringsAsFactors = FALSE
  )
  trimmed.added.data.df <- add.new.sca.data(trimmed.df)
  
  # replace was.dismissed with was.litigated
  reduced.df <- subset(trimmed.added.data.df, was.dismissed != 'ONGOING')
  reduced.df$was.litigated <- TRUE
  reduced.df$was.dismissed <- NULL
  return(reduced.df)
}

add.new.sca.data <- function(sca.df) {
  sca.df$search <- paste(sca.df$tic, sca.df$year, sep="-")
  sca.df$settlement[match('DBD-2010', sca.df$search)] <- 31600000
  sca.df$settlement[match('DELL-2006', sca.df$search)] <- 40000000
  sca.df$settlement[match('CELL-2006', sca.df$search)] <- 10000000
  sca.df$settlement[match('AVID-2013', sca.df$search)] <- 2600000
  sca.df$settlement[match('AXST-2013', sca.df$search)] <- 1250000
  sca.df$settlement[match('CLS-2007', sca.df$search)] <- 30000000
  sca.df$settlement[match('CLS-2015', sca.df$search)] <- 50000000
  sca.df$settlement[match('MXWL-2013', sca.df$search)] <- 3300000
  sca.df$settlement[match('OSIS-2013', sca.df$search)] <- 15000000
  sca.df$settlement[match('PLCM-2013', sca.df$search)] <- 750000
  sca.df$settlement[match('SIHI-2012', sca.df$search)] <- 600000
  sca.df$settlement[match('UBNT-2012', sca.df$search)] <- 6800000
  sca.df$settlement[match('VMEM-2013', sca.df$search)] <- 7500000
  sca.df <- subset(sca.df, search != 'ICXT-2010')
  sca.df <- subset(sca.df, search != 'DELL-2014')
  sca.df <- subset(sca.df, search != 'DELL-2006')
  sca.df <- subset(sca.df, search != 'ELX-2015')
  sca.df <- subset(sca.df, search != 'CELL-2012')
  sca.df$search <- NULL
  return(sca.df)
}


# add.rating.to.securities <- function(securities.df, rating.df) {
#   
# }









