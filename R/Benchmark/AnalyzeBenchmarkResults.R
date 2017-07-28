library(ggplot2)
library(reshape2)

consumerFile <- "consumer.csv"
producerFile <- "producer.csv"

consumerRaw <- read.csv(consumerFile, header = TRUE, sep = ",")
producerRaw <- read.csv(producerFile, header = TRUE, sep = ",")


unifiedData <- merge(aggregate(. ~ Experiment.Name, consumerRaw, mean), 
                     aggregate(. ~ Experiment.Name, producerRaw, mean),
                     by = "Experiment.Name")
unifiedData$All.Received. <- as.logical(unifiedData$All.Received. - 1)
unifiedData$Number.of.Shards <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][1])
unifiedData$Number.of.Shards <- as.numeric(unifiedData$Number.of.Shards)
unifiedData$Throughput.Limit <- unifiedData$Number.of.Shards * 1000

# Is all message received?
ggplot(unifiedData, aes(x = All.Received.)) + geom_histogram(stat = "count") + ggtitle("Is all messages received?")

# IS all message receivid, group by number of shard
ggplot(unifiedData, aes(x = factor(Number.of.Shards), fill = All.Received.)) + geom_bar(stat = "count")

# Is all messages received if throughput <= limit throughput
ggplot(subset(unifiedData, Thoughput < 1000 * Number.of.Shards), aes(x = factor(Number.of.Shards), fill = All.Received.)) + geom_bar(stat = "count")

# Is there any error (IN PRODUCER)
ggplot(unifiedData, aes(x = factor(Number.of.Shards), fill = Errors == 0)) + geom_bar(stat = "count")

# Is there any error if thoughput <= limit throuput (IN PRODUCER)
ggplot(subset(unifiedData, Thoughput < 1000 * Number.of.Shards), aes(x = factor(Number.of.Shards), fill = Errors == 0)) + geom_bar(stat = "count")

# Maxmimum Through put each number of shards that no Error in producer
aggregate(Thoughput ~ Number.of.Shards, subset(unifiedData, (Errors == 0)), max)

# Maxmimum Through put each number of shards that all messageds received in consumer
aggregate(Thoughput ~ Number.of.Shards, subset(unifiedData, (All.Received.)), max)

# Maxmimum Through put each number of shards that all messageds received in consumer && No Erros in producer
noErrorAllReceived <- aggregate(Thoughput ~ Number.of.Shards, subset(unifiedData, (All.Received. & (Errors == 0))), max)
noErrorAllReceived <- merge(noErrorAllReceived, unifiedData, by = c("Number.of.Shards", "Thoughput"))
noErrorAllReceived <- noErrorAllReceived[order(noErrorAllReceived$Number.of.Shards),]
noErrorAllReceived

# Plot no error all received by # of shards
moltenNEAR <- melt(noErrorAllReceived[,c("Number.of.Shards", "Thoughput", "Throughput.Limit")], id = "Number.of.Shards")
ggplot(moltenNEAR, aes(x = factor(Number.of.Shards), y = value, fill = variable)) + 
  geom_bar(stat = "identity", position = position_dodge())

# Throuput vs Latency in all settings: 
ggplot(subset(unifiedData), aes(x = Latency.Average, y = Thoughput, color = All.Received.)) + geom_point()

# Throuput vs Latency in each settings:
ggplot(subset(unifiedData), aes(x = Latency.Average, y = Thoughput, color = All.Received.)) + geom_point() + facet_wrap(~ Number.of.Shards)

