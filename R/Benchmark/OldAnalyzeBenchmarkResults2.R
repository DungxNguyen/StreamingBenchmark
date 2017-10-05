library(ggplot2)
library(reshape2)

consumerFile <- "consumer2.csv"
producerFile <- "producer2.csv"

consumerRaw <- read.csv(consumerFile, header = TRUE, sep = ",")
producerRaw <- read.csv(producerFile, header = TRUE, sep = ",")


unifiedData <- merge(aggregate(. ~ Experiment.Name, consumerRaw, mean), 
                     aggregate(. ~ Experiment.Name, producerRaw, mean),
                     by = "Experiment.Name")
# Kafka
unifiedData$All.Received. <- as.logical(unifiedData$All.Received. - 1)
unifiedData$Number.of.Shards <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][4])
unifiedData$Number.of.Shards <- as.numeric(unifiedData$Number.of.Shards)
unifiedData$Throughput.Limit <- unifiedData$Number.of.Shards * 1024 * 1024
unifiedData$Throughput.Produce <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][1])
unifiedData$Throughput.Produce <- as.numeric(unifiedData$Throughput.Produce)
noErrorAllReceived <- unifiedData

##Kinesis
#unifiedData$All.Received. <- as.logical(unifiedData$All.Received. - 1)
#unifiedData$Number.of.Shards <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][2])
#unifiedData$Number.of.Shards <- as.numeric(unifiedData$Number.of.Shards)
#unifiedData$Throughput.Limit <- unifiedData$Number.of.Shards * 1000
#unifiedData$Throughput.Produce <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][4])
#unifiedData$Throughput.Produce <- as.numeric(unifiedData$Throughput.Produce)
#unifiedData$Duration <- unifiedData$Duration - 120

# Is all message received?
ggplot(unifiedData, aes(x = All.Received.)) + geom_histogram(stat = "count") + ggtitle("Is all messages received?")

# IS all message received, group by number of shard
ggplot(unifiedData, aes(x = factor(Number.of.Shards), fill = All.Received.)) + geom_bar(stat = "count")


# Maxmimum Throughput each number of shards that all messageds received in consumer && No Erros in producer
# Kinesis
# noErrorAllReceived <- aggregate(Throughput ~ Number.of.Shards, subset(unifiedData, All.Received. ), max)
# noErrorAllReceived <- merge(noErrorAllReceived, unifiedData, by = c("Number.of.Shards", "Thoughput"))
# noErrorAllReceived <- noErrorAllReceived[order(noErrorAllReceived$Number.of.Shards),]
# noErrorAllReceived


# Plot all received by # of shards
colnames(noErrorAllReceived)[which(names(noErrorAllReceived) == "Throughput")] <- "Message Producer Sending Rate"
colnames(noErrorAllReceived)[which(names(noErrorAllReceived) == "Throughput.Limit")] <- "Threotical Throughput Limit of Kinesis with same shards"
moltenNEAR <- melt(noErrorAllReceived[,c("Number.of.Shards", "Message Producer Sending Rate", "Threotical Throughput Limit of Kinesis with same shards")], id = "Number.of.Shards")
ggplot(moltenNEAR, aes(x = factor(Number.of.Shards), y = value, fill = variable)) + 
  geom_bar(stat = "identity", position = position_dodge()) +
  xlab("Count of Shards") +
  ylab("Rate (KB/s)")

# Need debug  here
unifiedData$throughputRatio <- unifiedData$Thoughput / unifiedData$Throughput.Limit 
ggplot(unifiedData, aes(x=factor(Number.of.Shards), y = throughputRatio)) + geom_bar(stat = "identity")

# Plot Produce Throughput and Sending Throughput Consumer Throughput and  
plotData <- unifiedData
colnames(plotData)[which(names(plotData) == "Throughput")] <- "Message Producer Sending Rate"
plotData$Throughput.Average <- plotData$Throughput.Average * 1024
colnames(plotData)[which(names(plotData) == "Throughput.Average")] <- "Message Consumer Rate"
molten <- melt(plotData[,c("Number.of.Shards", "Throughput.Produce", "Message Producer Sending Rate", "Message Consumer Rate")], id = c("Number.of.Shards", "Throughput.Produce")) 
ggplot(molten, aes(x = factor(Throughput.Produce), y = value, fill = variable)) + 
  geom_bar(stat = "identity", position = position_dodge()) + facet_wrap(~Number.of.Shards) +
  xlab("Message Generation Rate") +
  ylab("Rate (KB/s)")

# Produce Throuput vs Latency in each settings:
ggplot(subset(unifiedData), aes(x = log(Latency.Average), y = Throughput.Produce, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

# Produce Throuput vs Latency Coeff of var in each settings:
ggplot(subset(unifiedData), aes(x = Latency.SD/Latency.Average, y = Throughput.Produce, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

# Produce Throuput vs Latency Max in each settings:
ggplot(subset(unifiedData), aes(x = log(Latency.Max), y = Throughput.Produce, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

# Produce Throuput vs Latency in 32 shards settings:
ggplot(subset(unifiedData, Number.of.Shards == 32), aes(x = Latency.Average, y = Throughput.Produce, color = All.Received.)) + geom_point()

# Latency of shard by Produce Throughput
ggplot(subset(unifiedData), aes(y = Latency.Average, x = factor(Number.of.Shards))) + geom_bar(stat = "identity") + facet_wrap(~ Throughput.Produce)

# Duration of processing time for all settings, split by Produce Throughput;
ggplot(subset(unifiedData), aes(y = Duration, x = factor(Number.of.Shards)), color = All.Received.) + geom_bar(stat = "identity") + facet_wrap(~ Throughput.Produce)

# Duration of processing time for all settings, split by Number of Shards;
ggplot(subset(unifiedData), aes(y = Duration, x = factor(Throughput.Produce)), color = All.Received.) + geom_bar(stat = "identity") + facet_wrap(~ Number.of.Shards)

# Buffering time in each setting:
ggplot(subset(unifiedData), aes(x = Buffering.Time, y = Throughput.Produce, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

# Producer sending fail rate in each setting:
ggplot(subset(unifiedData), aes(x = Errors/Records, y = Throughput.Produce, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

# What's error  ?