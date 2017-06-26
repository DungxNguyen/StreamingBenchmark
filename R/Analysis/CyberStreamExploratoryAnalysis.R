library(ggplot2)

# CONSTANT VARIABLES

# Number of sample
NUMBER_OF_SAMPLES <- 1000



# END OF CONSTANT VARIABLES



sparkData <- NULL

if(Sys.info()["sysname"] != "Linux") {
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
      spark.dynamicAllocation.maxExecutors = "1000",
      spark.executor.cores = "3",
      spark.executor.memory = "36g",
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
    invalid_vin = runif(NUMBER_OF_SAMPLES),
    vehicle_id = runif(NUMBER_OF_SAMPLES)
  )
  
  # Convert R dataframe to Spark DataFrame
  sparkData <- as.DataFrame(data, numPartitions = 7)
}

# Schema: fields and their types
printSchema(sparkData)

# Count the number of records
count = head(count(sparkData))
count

# Count the records by date
groupedByDateCount = collect(count(groupBy(sparkData, "date")))
byDateHistPlot <- ggplot(data = groupedByDateCount, aes(x = date, y = count))
byDateHistPlot + geom_point() + geom_line() + geom_smooth() + ggtitle("Histogram By Day")

# Count some distinct 
distinctVin <- collect(select(sparkData, countDistinct(sparkData$invalid_vin)))
distinctVehicle <- collect(select(sparkData, countDistinct(sparkData$vehicle_id)))
distinctCat <- collect(select(sparkData, countDistinct(sparkData$cat)))

# Get the distinct values of a colunm
distinctCatValues <- collect(distinct(select(sparkData, "cat")))
distinctCatValues

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
head(levelCatCombinationCountDesc)
