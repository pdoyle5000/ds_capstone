# Data ETL Functions
library(dplyr)
library(lubridate)

import.market.data <- function(securities.path, 
                               ratings.path, 
                               stocks.path, 
                               sca.path, 
                               fundamentals.path) {
  fundamentals.df <- etl.fundamentals(read.csv(fundamentals.path, sep = ','))
  ratings.df <- etl.ratings(read.csv(ratings.path, sep = ','))
  
  ## COMMENTED OUT FOR SPEED FOR NOW
  stocks.df <- etl.stocks(read.csv(stocks.path, sep = '\t', header = T, fileEncoding = 'UTF-16LE'), securities.path)
  ## --------------------------- etl.stocks takes a very long time.
  
  
  stocks1.df <- unified.stocks.df
  sca.df <- etl.sca(read.csv(sca.path, sep = ','), fundamentals.df$tic)
  
  unified.df <- join.dfs(fundamentals.df, ratings.df, stocks1.df, sca.df)
  
  
  unified.df$data.month <- NULL
  unified.df$eps.basic <- NULL
  unified.df$market.cap <- unified.df$price.close.monthly * unified.df$shares.outstanding.common
  unified.df$settlement.dollars <- unified.df$settlement
  
  unified.df <- subset(unified.df, shares.outstanding.common > 0)
  unified.df <- subset(unified.df, tic != 'MTRO')
  unified.df <- subset(unified.df, tic != 'VOLT')
  unified.df$settlement <- ((unified.df$settlement * .000001) / unified.df$market.cap)
  unified.df$mpl <- abs((unified.df$stock.price.trend * unified.df$shares.outstanding.common) / unified.df$market.cap)
  unified.df$earnings <- abs((unified.df$eps * unified.df$shares.outstanding.common) / unified.df$market.cap)

  return(unified.df)
}

join.dfs <- function(df1, df2, df3, df4) {
  total.df <- df1 %>% left_join(df2, by=c('tic', 'year')) %>% 
           left_join(df3, by=c('tic', 'year')) %>% 
           left_join(df4, by=c('tic', 'year'))
  
  # clean up company rating join
  total.df$company.rating[is.na(total.df$company.rating)] <- 0
  total.df$quality.ranking[is.na(total.df$quality.ranking)] <- 0
  
  total.df$quality.rating <- total.df$company.rating
  total.df$quality.rating <- ifelse(total.df$quality.rating == 0, total.df$fundamentals.rating, total.df$quality.ranking)

  total.df$company.rating <- NULL
  total.df$fundamentals.rating <- NULL
  total.df$quality.ranking <- NULL
  
  
  # clean up stocks join
  total.df$eps[is.na(total.df$eps)] <- 0
  total.df$eps <- ifelse(total.df$eps.basic == 0, total.df$eps, total.df$eps.basic)
  total.df <- subset(total.df, eps != 0)
  total.df$shares.outstanding <- NULL
  total.df <- subset(total.df, year > 2009)
  total.df <- subset(total.df, !is.na(stock.price.trend))
  
  
  # clean up litigations join
  total.df$was.litigated[is.na(total.df$was.litigated)] <- FALSE
  total.df$settlement[is.na(total.df$settlement)] <- 0
  
  return(total.df)
}

etl.ratings <- function(ratings.df) {
  trimmed.ratings.df <- data.frame(
    'tic' =            as.character(ratings.df$tic),
    'year' =           year(as.Date(ratings.df$datadate, format = "%m/%d/%y")),
    'company.rating' = as.character(ratings.df$spcsrc),
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
    if(startsWith(rating, 'L')) {
      score <- -1
    }
    if(is.na(rating)) {
      score <- 0
    }
    scores <- c(scores, score)
  }
  return(scores)
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



etl.stocks <- function(stocks.df, securities.path) {
  trimmed.df <- data.frame(
    "tic" =                 as.character(stocks.df$tic),
    "year" =                year(as.Date(stocks.df$datadate, format = "%m/%d/%Y")),
    "data.month" =          month(as.Date(stocks.df$datadate, format = "%m/%d/%Y")),
    "current.eps" =         stocks.df$eps,
    "shares.outstanding" =  stocks.df$cshoc,
    "quality.ranking" =     determine.rating.score(stocks.df$spcsrc),
    "trade.volume" =        stocks.df$cshtrd,
    "closing.price" =       stocks.df$prccd,
    stringsAsFactors=FALSE)
  
  trimmed.df[(is.na(trimmed.df))] <- 0
  monthly.stock.df <- trimmed.df %>% group_by(tic, year, data.month) %>% summarize(mean.current.eps = mean(as.numeric(current.eps)),
                                                                                   mean.shares.outstanding = mean(as.numeric(shares.outstanding)),
                                                                                   mean.quality.ranking = mean(as.numeric(quality.ranking)),
                                                                                   trading.volume = sum(as.numeric(trade.volume)),
                                                                                   prccm = mean(as.numeric(closing.price)))
  
  yearly.stock.df <- monthly.stock.df %>% group_by(tic, year) %>% summarize(current.eps.stock = mean(as.numeric(mean.current.eps)),
                                                                            shares.outstanding.stock = mean(as.numeric(mean.shares.outstanding)),
                                                                            quality.ranking.stock = mean(as.numeric(mean.quality.ranking)),
                                                                            trading.volume.stock = sum(as.numeric(trading.volume)),
                                                                            prccm = mean(as.numeric(prccm)))
  
  yearly.w.trends.df <- calc.trends.stocks(yearly.stock.df, monthly.stock.df)
  sec.w.trends.df <- etl.securities(read.csv(securities.path, sep = ','))
  
  joined.stock.securities <- yearly.w.trends.df %>% full_join(sec.w.trends.df, by=c('tic', 'year'))
  joined.stock.securities <- subset(joined.stock.securities, !is.na(current.eps.stock))
  
  unified.stocks.df <- data.frame(
    'tic' = joined.stock.securities$tic,
    'year' = joined.stock.securities$year,
    'eps' = joined.stock.securities$current.eps.stock,
    'trade.volume' = ifelse(joined.stock.securities$trading.volume.stock > 0, joined.stock.securities$trading.volume.stock, joined.stock.securities$trade.volume),
    'stock.price.trend' = joined.stock.securities$price.trend.stock,
    'price.close.monthly' = joined.stock.securities$prccm,
    'shares.outstanding' = joined.stock.securities$shares.outstanding.stock,
    'quality.ranking' = joined.stock.securities$quality.ranking.stock,
    stringsAsFactors = F
  )
  return(subset(unified.stocks.df, !is.na(trade.volume)))
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

calc.trends.stocks <- function(yearly.df, monthly.df) {
  yearly.df[, 'price.trend.stock'] <- NA
  for(row in 1:nrow(yearly.df)) {
    # for this tic and year, subset the monthly.df on the same tic and year.
    # then sort by date and get price close[last] - price close[first]
    company <- yearly.df$tic[row]
    this_year <- yearly.df$year[row]
    temp.df <- subset(monthly.df, as.character(tic) == company & year == this_year)
    ordered.temp.df <- temp.df[order(temp.df$data.month),]
    closing.price.trend <- tail(ordered.temp.df$prccm, 1) - head(ordered.temp.df$prccm, 1)
    yearly.df$price.trend.stock[row] <- closing.price.trend
  }
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
  trimmed.added.data.df$was.litigated <- TRUE
  trimmed.added.data.df$was.dismissed <- NULL
  return(trimmed.added.data.df)
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

etl.fundamentals <- function(raw.df) {
  fund.df <- subset(raw.df, datafmt == 'STD')
  fund.df <- subset(fund.df, !is.na(aco))
  fund.df[is.na(fund.df)] <- 0
  fund.df$spcsrc <- determine.rating.score(fund.df$spcsrc)
  
  fund.df$tic <- as.character(fund.df$tic)
  cleaned.fund.df <- data.frame(
    'tic' = fund.df$tic,
    'year' = fund.df$fyear,
    'data.month' = month(as.Date(fund.df$datadate, format = "%m/%d/%y")),
    'assets.total' = fund.df$at,
    'cash' = fund.df$ch,
    'shares.outstanding.common' = fund.df$csho,
    'co.stock' = fund.df$cstk,
    'eps.basic' = fund.df$epspi,
    #'eps.diluted' = fund.df$epsfi,
    'financing.net.cash.flow' = fund.df$fincf,
    'gross.profit.loss' = fund.df$gp,
    'invested.capital' = fund.df$icapt,
    'total.inventories' = fund.df$invt,
    'liabilities.total' = fund.df$lt,
    'net.income.loss' = fund.df$ni,
    'volatility.assumption' = fund.df$optvol,
    'risk.free.rate' = fund.df$optrfr,
    'retained.earnings' = fund.df$re,
    'retained.earnings.restatement' = fund.df$rea,
    'revenue.total' = fund.df$revt,
    'working.capital' = fund.df$wcap,
    'deffered.long.asset.tax' = fund.df$txdba,
    'net.deferred.tax.asset' = fund.df$txndb,
    'fundamentals.rating' = fund.df$spcsrc,
    stringsAsFactors = F
  )
  return(cleaned.fund.df)
}