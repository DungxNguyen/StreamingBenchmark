Sys.setenv(SPARK_HOME = "/home/dnguyen/spark-2.1.1-bin-hadoop2.7")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

# Create R data frame
data <- data.frame(id = 1:100,
                   Date = base::sample(seq(as.Date("2017-01-01"), as.Date("2017-01-31"), by = "day"), 100, replace = TRUE),
                   level = ifelse(runif(10)<0.3, "WARN", "ERROR"))

# Order by Date
data <- data[with(data, order(Date)),]

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
