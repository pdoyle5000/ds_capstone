# Code to keep for reference:

# # Next create a new df converting years to columns and return.
# per.year.df <- data.frame("tic" = character(),
#                           "mean.trade.volume.2010" = numeric(),
#                           "mean.trade.volume.2011" = numeric(),
#                           "mean.trade.volume.2012" = numeric(),
#                           "mean.trade.volume.2013" = numeric(),
#                           "mean.trade.volume.2014" = numeric(),
#                           "mean.closing.price.2010" = numeric(),
#                           "mean.closing.price.2011" = numeric(),
#                           "mean.closing.price.2012" = numeric(),
#                           "mean.closing.price.2013" = numeric(),
#                           "mean.closing.price.2014" = numeric(),
#                           "high.price.2010" = numeric(),
#                           "high.price.2011" = numeric(),
#                           "high.price.2012" = numeric(),
#                           "high.price.2013" = numeric(),
#                           "high.price.2014" = numeric(),
#                           "low.price.2010" = numeric(),
#                           "low.price.2011" = numeric(),
#                           "low.price.2012" = numeric(),
#                           "low.price.2013" = numeric(),
#                           "low.price.2014" = numeric(),
#                           stringsAsFactors=FALSE)
# unique.companies <- unique(summary.df$tic)
# for(company in unique.companies) {
#   company.records <- subset(summary.df, tic == company)
#   # Missing data is indicative of a company not being public.  Zero is acceptable
#   # because it is accurate, the companies were not trading.
#   df.entry <- data.frame("tic" = as.character(company),
#                             "mean.trade.volume.2010" = 0,
#                             "mean.trade.volume.2011" = 0,
#                             "mean.trade.volume.2012" = 0,
#                             "mean.trade.volume.2013" = 0,
#                             "mean.trade.volume.2014" = 0,
#                             "mean.closing.price.2010" = 0,
#                             "mean.closing.price.2011" = 0,
#                             "mean.closing.price.2012" = 0,
#                             "mean.closing.price.2013" = 0,
#                             "mean.closing.price.2014" = 0,
#                             "high.price.2010" = 0,
#                             "high.price.2011" = 0,
#                             "high.price.2012" = 0,
#                             "high.price.2013" = 0,
#                             "high.price.2014" = 0,
#                             "low.price.2010" = 0,
#                             "low.price.2011" = 0,
#                             "low.price.2012" = 0,
#                             "low.price.2013" = 0,
#                             "low.price.2014" = 0,
#                             stringsAsFactors=FALSE)
#   for (row in 1:nrow(company.records)) {
#     if(company.records$year[row] == "2010") {
#       df.entry$mean.trade.volume.2010[1] <- company.records$trade.volume_mean[row]
#       df.entry$high.price.2010[1] <- company.records$price.high_max[row]
#       df.entry$low.price.2010[1] <- company.records$price.low_min[row]
#       df.entry$mean.closing.price.2010[1] <- company.records$price.close_mean[row]
#     }
#     if(company.records$year[row] == "2011") {
#       df.entry$mean.trade.volume.2011[1] <- company.records$trade.volume_mean[row]
#       df.entry$high.price.2011[1] <- company.records$price.high_max[row]
#       df.entry$low.price.2011[1] <- company.records$price.low_min[row]
#       df.entry$mean.closing.price.2011[1] <- company.records$price.close_mean[row]
#     }
#     if(company.records$year[row] == "2012") {
#       df.entry$mean.trade.volume.2012[1] <- company.records$trade.volume_mean[row]
#       df.entry$high.price.2012[1] <- company.records$price.high_max[row]
#       df.entry$low.price.2012[1] <- company.records$price.low_min[row]
#       df.entry$mean.closing.price.2012[1] <- company.records$price.close_mean[row]
#     }
#     if(company.records$year[row] == "2013") {
#       df.entry$mean.trade.volume.2013[1] <- company.records$trade.volume_mean[row]
#       df.entry$high.price.2013[1] <- company.records$price.high_max[row]
#       df.entry$low.price.2013[1] <- company.records$price.low_min[row]
#       df.entry$mean.closing.price.2013[1] <- company.records$price.close_mean[row]
#     }
#     if(company.records$year[row] == "2014") {
#       df.entry$mean.trade.volume.2014[1] <- company.records$trade.volume_mean[row]
#       df.entry$high.price.2014[1] <- company.records$price.high_max[row]
#       df.entry$low.price.2014[1] <- company.records$price.low_min[row]
#       df.entry$mean.closing.price.2014[1] <- company.records$price.close_mean[row]
#     }
#   }
#   per.year.df <- rbind(per.year.df, df.entry)
# }
# return(per.year.df)