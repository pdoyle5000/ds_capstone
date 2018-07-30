# Data ETL Functions
library(dplyr)
library(lubridate)

import.market.data <- function(securities.path, 
                               ratings.path, 
                               stocks.path, 
                               sca.path, 
                               fundamentals.path) {
  
  securities.df <- etl.securities(read.csv(securities.path, sep = ','))
  ratings.df <- etl.ratings(read.csv(ratings.path, sep = ','))
  stocks.df <- etl.stocks(read.csv(stocks.path, sep = '\t', header = T, fileEncoding = 'UTF-16LE'))
  sca.df <- etl.sca(read.csv(sca.path, sep = ','), securities.df$tic)
  fundamentals.df <- etl.fundamentals(read.csv(fundamentals.path, sep = ','))
  unified.df <- join.dfs(securities.df, ratings.df, stocks.df, sca.df, fundamentals.df)
  
  unified.df$settlement.pct <- (unified.df$settlement / (unified.df$price.close * unified.df$shares.outstanding)) * 100
  
  # bring in fundamentals and wittle down the columns.
  # only 41% of the companies are rated. Maybe better off as rated_good/rated_bad/unrated
  # analysis can begin to create normality!!!
  
  return(unified.df)
}

join.dfs <- function(df1, df2, df3, df4, df5) {
  return(df1 %>% full_join(df2, by=c('tic', 'year')) %>% 
           full_join(df3, by=c('tic', 'year')) %>% 
           full_join(df4, by=c('tic', 'year')) %>%
           full_join(df5, by=c('tic', 'year')))
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
  fund.df$spcsrc <- determine.rating.score(fund.df$spcsrc)
  fund.df[is.na(fund.df)] <- 0
  fund.df <- remove.meaningless.data(fund.df)
  fund.df$datadate <- NULL
  colnames(fund.df)[colnames(fund.df) == 'fyear'] <- 'year'
  fund.df$tic <- as.character(fund.df$tic)
  return(fund.df)
}

remove.meaningless.data <- function(fun.df) {
  fun.df$ceql <- NULL
  fun.df$acox <- NULL
  fun.df$aox <- NULL
  fun.df$act <- NULL
  fun.df$aodo <- NULL
  fun.df$aco <- NULL
  fun.df$ceqt <- NULL
  fun.df$ceq <- NULL
  fun.df$ci <- NULL
  fun.df$ch <- NULL
  fun.df$ap <- NULL
  fun.df$capx <- NULL
  fun.df$capxv <- NULL
  fun.df$aocipen <- NULL
  fun.df$am <- NULL
  fun.df$ao <- NULL
  fun.df$aol2 <- NULL
  fun.df$citotal <- NULL
  fun.df$cibegni <- NULL
  fun.df$cshfd <- NULL
  fun.df$cshpri <- NULL
  fun.df$cogs <- NULL
  fun.df$csho <- NULL
  fun.df$che <- NULL
  fun.df$cshi <- NULL
  fun.df$aqpl1 <- NULL
  fun.df$aocisecgl <- NULL
  fun.df$ano <- NULL
  fun.df$aocidergl <- NULL
  fun.df$aociother <- NULL
  fun.df$aocisecgl <- NULL
  fun.df$aqc <- NULL
  fun.df$aocidergl <- NULL
  fun.df$cipen <- NULL
  fun.df$cisecgl <- NULL
  fun.df$cld2 <- NULL
  fun.df$cld3 <- NULL
  fun.df$cld4 <- NULL
  fun.df$cld5 <- NULL
  fun.df$cicurr <- NULL
  fun.df$cidergl<- NULL
  fun.df$cimii <- NULL
  fun.df$ciother <- NULL
  fun.df$aul3 <- NULL
  fun.df$apalch <- NULL
  fun.df$dd1 <- NULL
  fun.df$dd2 <- NULL
  fun.df$dd3 <- NULL
  fun.df$dd4 <- NULL
  fun.df$dd5 <- NULL
  fun.df$dd <- NULL
  fun.df$dcvt <- NULL
  fun.df$dcvsub <- NULL
  fun.df$dcvsr <- NULL
  fun.df$dcs <- NULL
  fun.df$dcpstk <- NULL
  fun.df$dcom <- NULL
  fun.df$dclo <- NULL
  fun.df$diladj <- NULL
  fun.df$dc <- NULL
  fun.df$cdtke <- NULL
  fun.df$dilavx <- NULL
  fun.df$cstkcv <- NULL
  fun.df$cstke <- NULL
  fun.df$intc <- NULL
  fun.df$invo <- NULL
  fun.df$itcb <- NULL
  fun.df$itci <- NULL
  fun.df$ivaeq <- NULL
  fun.df$ivao <- NULL
  fun.df$ivch <- NULL
  fun.df$ivstch <- NULL
  fun.df$ibcom <- NULL
  fun.df$ibadj <- NULL
  fun.df$epspi <- NULL
  fun.df$ibmii <- NULL
  fun.df$ibc <- NULL
  fun.df$ibcom <- NULL
  fun.df$epspx <- NULL
  fun.df$fopox <- NULL
  fun.df$dpc <- NULL
  fun.df$ib <- NULL
  fun.df$gdwl <- NULL
  fun.df$ebitda <- NULL
  fun.df$gp <- NULL
  fun.df$icapt <- NULL
  fun.df$lco <- NULL
  fun.df$lcox <- NULL
  fun.df$epsfx <- NULL
  fun.df$lct <- NULL
  fun.df$dp <- NULL
  fun.df$intano <- NULL
  fun.df$ebit <- NULL
  fun.df$invfg <- NULL
  fun.df$invwip <- NULL
  fun.df$emp <- NULL
  fun.df$dlc <- NULL
  fun.df$dpact <- NULL
  fun.df$dpvieb <- NULL
  fun.df$ivst <- NULL
  fun.df$lcoxdr <- NULL
  fun.df$intpn <- NULL
  fun.df$intan <- NULL
  fun.df$invrm <- NULL
  fun.df$ivncf <- NULL
  fun.df$epsfi <- NULL
  fun.df$invt <- NULL
  fun.df$ob <- NULL
  fun.df$np <- NULL
  fun.df$msa <- NULL
  fun.df$mii <- NULL
  fun.df$mibt <- NULL
  fun.df$lifr <- NULL
  fun.df$lno <- NULL
  fun.df$lol2 <- NULL
  fun.df$lqpl1 <- NULL
  fun.df$lul3 <- NULL
  fun.df$mib <- NULL
  fun.df$mibn <- NULL
  fun.df$niadj <- NULL
  fun.df$mrc1 <- NULL
  fun.df$mrc2 <- NULL
  fun.df$mrc3 <- NULL
  fun.df$mrc4 <- NULL
  fun.df$mrc5 <- NULL
  fun.df$oibdp <- NULL
  fun.df$nopio <- NULL
  fun.df$oiadp <- NULL
  fun.df$oancf <- NULL
  fun.df$loxdr <- NULL
  fun.df$lt <- NULL
  fun.df$mrct <- NULL
  fun.df$mrcta <- NULL
  fun.df$lo <- NULL
  fun.df$ni <- NULL
  fun.df$prca <- NULL
  fun.df$pnrsho <- NULL
  fun.df$pncaeps <- NULL
  fun.df$pncad <- NULL
  fun.df$pnca <- NULL
  fun.df$optdr <- NULL
  fun.df$pi <- NULL
  fun.df$oprepsx <- NULL
  fun.df$optosby <- NULL
  fun.df$optosey <- NULL
  fun.df$ppeveb <- NULL
  fun.df$optprcwa <- NULL
  fun.df$optprcwa <- NULL
  fun.df$optprcby <- NULL
  fun.df$pifo <- NULL
  fun.df$ppegt <- NULL
  fun.df$optexd <- NULL
  fun.df$optfvgr <- NULL
  fun.df$pidom <- NULL
  fun.df$ppent <- NULL
  fun.df$idit <- NULL
  fun.df$optprcex <- NULL
  fun.df$optlife <- NULL
  fun.df$optca <- NULL
  fun.df$optprcca <- NULL
  fun.df$optprcgr <- NULL
  fun.df$prcad <- NULL
  fun.df$prcaeps <- NULL
  fun.df$prsho <- NULL
  fun.df$prstkc <- NULL
  fun.df$pstk <- NULL
  fun.df$pstkc <- NULL
  fun.df$pstkl <- NULL
  fun.df$pstkn <- NULL
  fun.df$pstkr <- NULL
  fun.df$pstkrv <- NULL
  fun.df$rdip <- NULL
  fun.df$rdipa <- NULL
  fun.df$rdipd <- NULL
  fun.df$rdipeps <- NULL
  fun.df$rea <- NULL
  fun.df$reajo <- NULL
  fun.df$recco <- NULL
  fun.df$recd <- NULL
  fun.df$seqo <- NULL
  fun.df$siv <- NULL
  fun.df$sppe <- NULL
  fun.df$stkcpa <- NULL
  fun.df$spced <- NULL
  fun.df$reuna <- NULL
  fun.df$seq <- NULL
  fun.df$rectr <- NULL
  fun.df$spceeps <- NULL
  fun.df$revt <- NULL
  fun.df$sale <- NULL
  fun.df$tfva <- NULL
  fun.df$teq <- NULL
  fun.df$rect <- NULL
  fun.df$spce <- NULL
  fun.df$recta <- NULL
  fun.df$stkco <- NULL
  fun.df$spi <- NULL
  fun.df$tfvl <- NULL
  fun.df$tstk <- NULL
  fun.df$tstkc <- NULL
  fun.df$tstkn <- NULL
  fun.df$tstkp <- NULL
  fun.df$txach <- NULL
  fun.df$txbco <- NULL
  fun.df$txbcof <- NULL
  fun.df$txdb <- NULL
  fun.df$txdbcl <- NULL
  fun.df$txditc <- NULL
  fun.df$txds <- NULL
  fun.df$txdfed <- NULL
  fun.df$txdbca <- NULL
  fun.df$txc <- NULL
  fun.df$txdc <- NULL
  fun.df$txndbr <- NULL
  fun.df$txo <- NULL
  fun.df$txr <- NULL
  fun.df$txtubadjust <- NULL
  fun.df$txtubposdec <- NULL
  fun.df$txtubpospdec <- NULL
  fun.df$txtubpospinc <- NULL
  fun.df$txtubsettle <- NULL
  fun.df$txtubend <- NULL
  fun.df$txfed <- NULL
  fun.df$txpd <- NULL
  fun.df$txs <- NULL
  fun.df$txfo <- NULL
  fun.df$txndbl <- NULL
  fun.df$txndba <- NULL
  fun.df$txt <- NULL
  fun.df$txp <- NULL
  fun.df$txtubposinc <- NULL
  fun.df$txtubbegin <- NULL
  fun.df$xido <- NULL
  fun.df$xidoc <- NULL
  fun.df$txtubsoflimit <- NULL
  fun.df$txtubxintbs <- NULL
  fun.df$xopr <- NULL
  fun.df$xsga <- NULL
  fun.df$xacc <- NULL
  fun.df$xrd <- NULL
  fun.df$xrent <- NULL
  fun.df$wcap <- NULL
  fun.df$txtubtxtr <- NULL
  fun.df$xint <- NULL
  
  return(fun.df)
}






