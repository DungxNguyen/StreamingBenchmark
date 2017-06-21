library(ggplot2)
Sys.setenv(SPARK_HOME = "/home/dnguyen/spark-2.1.1-bin-hadoop2.7")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

NUMBER_OF_SAMPLES <- 100

timestamp <- sort(base::sample(seq(as.POSIXct("2017-01-01 00:00:00"), as.POSIXct("2017-01-31 23:59:59"), by = 1), NUMBER_OF_SAMPLES, replace = TRUE))
head(timestamp)

# Create R data frame
data <- data.frame(id = 1:NUMBER_OF_SAMPLES,
                   timestamp = timestamp,
                   Date = as.Date(timestamp),
                   level = ifelse(runif(NUMBER_OF_SAMPLES)<0.3, "WARN", "ERROR"))# Create R data frame

# Convert R dataframe to Spark DataFrame
sparkData <- as.DataFrame(data)

# View some top rows
head(sparkData)

# Print schema of data
schema(sparkData)

# Select column
collect(select(sparkData, sparkData$level))
collect(select(sparkData, "Date"))

# Select where ...
collect(filter(sparkData, sparkData$Date == "2017-01-13"))
collect(filter(sparkData, sparkData$Date %in% c("2017-01-15", "2017-01-20")))
collect(filter(sparkData, sparkData$Date %in% c("2017-01-15", "2017-01-20") & 
                          sparkData$level == "ERROR"))

# grouping
collect(summarize(groupBy(sparkData, sparkData$Date), count = n(sparkData$Date)))
collect(summarize(groupBy(sparkData, sparkData$level), count = n(sparkData$level) ))
collect(summarize(groupBy(sparkData, sparkData$Date, sparkData$level), count = n(sparkData$Date)))

# apply function, result is a R dataframe
applyCount <- dapplyCollect(sparkData, function(x){x <- cbind(x, paste(x$level, x$Date, sep = "_"))} )
head(applyCount)

# apply function, result is a spark DataFrame
# remember, result of inner function must be a data.frame
newSchema <- structType(structField("Value", "string"))
newData <- dapply(sparkData, function(x){data.frame(paste(x$Date, x$level, sep = "_"), stringsAsFactors = F)}, newSchema)
head(collect(newData))

# gapply is group apply
newSchema <- structType(structField("Date", "date"), structField("Value", "string"))
newData <- gapply(sparkData, "Date", function(key, x){data.frame(key, paste(x$level, collapse = "-"), stringsAsFactors = F)}, newSchema)
head(collect(newData))

# spark.lapply is lapply but distributed 
# spark.lapply run local R function

# SQL
createOrReplaceTempView(sparkData, "sparkDataTable")
collect(sql("SELECT * FROM sparkDataTable WHERE level == 'ERROR'"))

# count data by day
countByDay <- collect(count(group_by(sparkData, "Date")))
head(countByDay)
ggplot(data = countByDay, aes(x=Date, y=count)) + geom_line() + geom_point()

# count data by day and level
countByDayAndLevel <- collect(count(group_by(sparkData, "Date", "level")))
head(countByDayAndLevel)
ggplot(data = countByDayAndLevel, aes(x = Date, y = count, color = level)) + geom_point() + geom_line()

# count data by hour and level 
# create hour column
# care about type of data.frame
# it's easy to convert list to data.frame
hourConvert <- function(x){
  hour <- as.POSIXct(x$timestamp, origin = "1970-01-01")
  # type <- class(x$timestamp)
  # result <- as.data.frame(hour, stringsAsFactors = FALSE)
  # result <- data.frame( list(Hour = c("abc"), Count = as.integer(c(1))), stringsAsFactors = FALSE)
  # return(data.frame(list(as.character(type), as.integer(1)), stringsAsFactors = FALSE))
  hour <- format(hour, "%Y-%m-%D %H")
  hour <- as.data.frame(table(hour), stringsAsFactors = FALSE)
  return(hour)
}
hourSchema <- structType(structField("Hour", "string"), structField("Count", "integer"))
hourData <- dapply(sparkData, hourConvert, hourSchema)
collect(hourData)

# Function that create error
# Spark's Timestamp ---> R's numeric
# It may be a bug
# The real "can not serialize factor" error doesn't come with prettyNum exception
x <- data.frame(list(timestamp = c(1:100)))
hourConvertError <- function(x){
  as.data.frame(table(format(x$timestamp, "%Y-%m-%D %H")), stringsAsFactors = FALSE)  
}
hourConvertError(x)
hourData <- dapply(sparkData, hourConvertError, hourSchema)
collect(hourData)
# End of error code segment