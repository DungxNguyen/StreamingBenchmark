library(ggplot2)

# CONSTANT VARIABLES

# Number of sample
NUMBER_OF_SAMPLES <- 1000
NUMBER_OF_VEHICLES <- 20
NUMBER_OF_REQ <- 350


# END OF CONSTANT VARIABLES



sparkData <- NULL

if(Sys.info()["user"] == "qxs0269") {
  Sys.setenv(SPARK_HOME = "/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/spark/")
  Sys.setenv(HADOOP_CONF_DIR = "/etc/hive/conf")
  Sys.setenv(JAVA_HOME = "/usr/java/default/")
  Sys.setenv(SPARK_SUBMIT_ARGS = "--master yarn-client sparkr-shell")
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library(SparkR)
  
  sparkR.session(
    appName = "CyberStream Analysis R Studio",
    sparkConfig = list(
      spark.yarn.queue = "data",
      spark.network.timeout = "3600s",
      spark.driver.maxResultSize = "16g",
      spark.shuffle.service.enabled = "true",
      spark.dynamicAllocation.enabled = "true",
      spark.dynamicAllocation.maxExecutors = "70",
      spark.executor.cores = "3",
      spark.executor.memory = "24g",
      spark.r.driver.command = "/software/R-3.4.1/bin/Rscript",
      spark.r.command = "/software/R-3.4.1/bin/Rscript",
      #spark.executorEnv.PATH = "/opt/wakari/anaconda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/software/py3/bin:/usr/bin",
      #spark.r.use.daemon = "false",
      #spark.sparkr.use.daemon = "false",
      spark.r.heartBeatInterval = "3600s",
      spark.executor.heartbeatInterval = "3600s"
    )
  )
  
  sparkData <- read.df("/data/connected_drive_parquet/month=201705/dataset=b2vxfcdgwus")
  
} else {
  Sys.setenv(SPARK_HOME = "/home/dnguyen/spark-2.1.1-bin-hadoop2.7")
  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
  sparkR.session(master = "local[*]",
                 sparkConfig = list(spark.driver.memory = "2g"))
  
  timestamp <-
    sort(base::sample(
      seq(
        as.POSIXct("2017-01-01 00:00:00"),
        as.POSIXct("2017-01-31 23:59:59"),
        by = 1
      ),
      NUMBER_OF_SAMPLES,
      replace = TRUE
    ))
  
  head(timestamp)
  
  # Create R data frame
  data <- data.frame(
    id = 1:NUMBER_OF_SAMPLES,
    timestamp = timestamp,
    Date = as.Date(timestamp),
    level = ifelse(runif(NUMBER_OF_SAMPLES) < 0.3, "ERROR", "WARN"),
    cat = ifelse(runif(NUMBER_OF_SAMPLES) < 0.4, "APPLICATION", "WEB"),
    req = base::sample(NUMBER_OF_REQ, NUMBER_OF_SAMPLES, replace = TRUE),
    msg = runif(NUMBER_OF_SAMPLES),
    origin = runif(NUMBER_OF_SAMPLES)
  )
  data$vehicle_id <- ((data$req * 100003 + 89513) %% 521) %% NUMBER_OF_VEHICLES + 1
  data$invalid_vin <- data$vehicle_id * 95581 + 7583
  
  # Convert R dataframe to Spark DataFrame
  sparkData <- as.DataFrame(data, numPartitions = 7)
}

# Schema: fields and their types
printSchema(sparkData)

# Count the number of records
count <- head(count(sparkData))
count

# Count the records by date
groupedByDateCount <- collect(count(groupBy(sparkData, "date")))
byDateHistPlot <- ggplot(data = groupedByDateCount, aes(x = date, y = count))
byDateHistPlot + geom_point() + geom_line() + geom_smooth() + ggtitle("Histogram By Day")

# Count some distinct 
distinctVin <- collect(select(sparkData, countDistinct(sparkData$invalid_vin)))
distinctVehicle <- collect(select(sparkData, countDistinct(sparkData$vehicle_id)))
distinctCat <- collect(select(sparkData, countDistinct(sparkData$cat)))

# Get the distinct values of a colunm
distinctCatValues <- collect(distinct(select(sparkData, "cat")))
distinctCatValues

# Count # each level
levelCount <- head(count(groupBy(sparkData, "level")))
levelCount

# Get warning and error messages
dataWarnError <- select(filter(sparkData, sparkData$level != "INFO"), "timestamp")
dataWarnErrorCount <- head(count(dataWarnError))
dataWarnErrorSample <- head(dataWarnError, 10)

# Get error messages
dataError <- select(filter(sparkData, sparkData$level == "ERROR"), "*")
dataErrorCount <- head(count(dataError))
dataErrorSample <- head(dataError, 10)

# Hourly error/warn rate of month
hourConvert <- function(x){
  hour <- as.POSIXct(x$timestamp, origin = "1970-01-01")
  # type <- class(x$timestamp)
  # result <- as.data.frame(hour, stringsAsFactors = FALSE)
  # result <- data.frame( list(Hour = c("abc"), Count = as.integer(c(1))), stringsAsFactors = FALSE)
  # return(data.frame(list(as.character(type), as.integer(1)), stringsAsFactors = FALSE))
  hour <- format(hour, "%Y-%m-%D %H")
  hour <- as.data.frame(table(hour), stringsAsFactors = FALSE)
  # hour <- as.data.frame(list(hour), stringsAsFactors = FALSE)
  return(hour)
}
hourSchema <- structType(structField("Hour", "string"), structField("Count", "integer"))
hourTransform <- dapply(dataWarnError, hourConvert, hourSchema)
hourData <- summarize(groupBy(hourTransform, "Hour"), Count = "sum")
hourDataR <- collect(hourData)
names(hourDataR) <- c("Hour", "Count")
ggplot(data = hourDataR, aes(x = Hour, y = Count)) + geom_bar(stat = "identity")

# Hourly error/warn rate of day
# count hour in day
hourInDayConvert <- function(x){
  hour <- as.POSIXct(x$timestamp, origin = "1970-01-01")
  hour <- format(hour, "%H")
  hour <- as.data.frame(table(hour), stringsAsFactors = FALSE)
  return(hour)
}
hourSchema <- structType(structField("Hour", "string"), structField("Count", "integer"))
hourDataInDay <- summarize(groupBy(dapply(dataWarnError, hourInDayConvert, hourSchema), "Hour"), Count = "sum")
hourDataInDayR <- collect(hourDataInDay)
names(hourDataInDayR) <- c("Hour", "Count")
ggplot(data = hourDataInDayR, aes(x = Hour, y = Count)) + geom_bar(stat = "identity")

# Number of combinations level x category
levelCatCount <- select(sparkData, countDistinct(sparkData$cat, sparkData$level))
head(levelCatCount)

# List those combinations & Count the number of each combination between level and category
levelCatCombinationCount <- count(groupBy(sparkData, "level", "cat"))
levelCatCombinationCountDesc <- arrange(levelCatCombinationCount, desc(levelCatCombinationCount$count))
levelCatCombinationAll <- collect(levelCatCombinationCountDesc)

# Function to list combinations & count the number of each
Combinations <- function(data, ...){
  combinations <- count(groupBy(data, ...))
  combinations <- arrange(combinations, desc(combinations$count))
  collect(combinations)
}

Combinations(sparkData, "level", "cat", "comp")
Combinations(sparkData, "level", "cat", "origin")
Combinations(sparkData, "comp", "origin")
Combinations(sparkData, "level", "cat", "comp", "origin")

# Number of combinations level x category x origin
levelCatOriginCount <- select(sparkData, countDistinct(sparkData$cat, sparkData$level, sparkData$origin))
head(levelCatOriginCount)

# Number of combinations level x category x origin x comp
levelCatOriginCompCount <- select(sparkData, countDistinct(sparkData$cat, sparkData$level, 
                                                           sparkData$origin, sparkData$comp))
head(levelCatOriginCompCount)

# Number of combinations level x category x origin x comp x msg
levelCatOriginCompMsgCount <- select(sparkData, countDistinct(sparkData$cat, sparkData$level, 
                                                              sparkData$origin, sparkData$comp,
                                                              sparkData$msg))
head(levelCatOriginCompMsgCount)

# Count distint msg
head(select(sparkData, countDistinct(sparkData$msg)))
# Note: 300M records, 120M msg

# Top mesg
mesgCount <- count(groupBy(sparkData, "msg"))
mesgCountSample <- head(arrange(mesgCount, desc(mesgCount$count)), num = 20)
# 2 top mesg are 140M, each 74M and seems to be co-occur
# 3-4-5 seems to be co-occur too

# Top WARN/ERROR mesg
mesgWARNCount <- count(groupBy(filter(sparkData, sparkData$level != "INFO"), "msg"))
mesgWARNCountSample <- head(arrange(mesgWARNCount, desc(mesgWARNCount$count)), num = 20)

# Count number of vehicles
vehicleCount <- head(select(sparkData, countDistinct(sparkData$invalid_vin)))
vehicleCount

# Order vehicle by # of messages
vehicleDistinct <- agg(groupBy(sparkData, "invalid_vin"), count = n(sparkData$id))
vehicleDistinct <- arrange(vehicleDistinct, desc(vehicleDistinct$count))
head(vehicleDistinct)

# Count number of request
requestCount <- head(select(sparkData, countDistinct(sparkData$req)))
requestCount

# Count number of request per vehicles
requestPerVehicle <- collect(agg(groupBy(sparkData, "invalid_vin"), distinctReq = countDistinct(sparkData$req)))
head(requestPerVehicle)

# Some statistics about request/vehicles:
summary(requestPerVehicle$distinctReq)
sd(requestPerVehicle$distinctReq)
ggplot(data=requestPerVehicle) + geom_histogram(aes(x = distinctReq))

# Count records per request
recordPerRequest <- agg(groupBy(sparkData, "req"), count = count(sparkData$id))
recordStatistics <- collect(select(recordPerRequest, mean(recordPerRequest$count), sd(recordPerRequest$count)))
recordStatistics

# Count number of request per vehicles
filteredSparkData <- filter(sparkData, sparkData$invalid_vin != "NA")
requestPerVehicle <- agg(groupBy(filteredSparkData, "invalid_vin"), distinctReq = countDistinct(filteredSparkData$req))
requestPerVehicleR <- collect(requestPerVehicle)
