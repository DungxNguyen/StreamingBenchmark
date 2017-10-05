library(ggplot2)
library(reshape2)

consumerFile <- "consumer-high.csv"
producerFile <- "producer-high.csv"

consumerRaw <- read.csv(consumerFile, header = TRUE, sep = ",")
producerRaw <- read.csv(producerFile, header = TRUE, sep = ",")

# Kafka Consumer, rename Experiment ID
consumerRaw$Experiment.Name <- sapply(consumerRaw$Experiment.Name, function(x) paste(strsplit(as.character(x), split = "-", fixed = TRUE)[[1]][1:4], collapse = "-"))

unifiedData <- merge(aggregate(. ~ Experiment.Name, consumerRaw, mean), 
                     aggregate(. ~ Experiment.Name, producerRaw, mean),
                     by = "Experiment.Name")
unifiedData$All.Received. <- as.logical(unifiedData$All.Received.)
unifiedData$Number.of.Shards <- sapply(unifiedData$Experiment.Name, function(x) strsplit(as.character(x), split = "-", fixed =  TRUE)[[1]][4])
unifiedData$Number.of.Shards <- as.numeric(unifiedData$Number.of.Shards)
unifiedData$Throughput.Limit <- unifiedData$Number.of.Shards * 1024
unifiedData$Throughput <- unifiedData$Throughput / 1024

# Is all message received?
table(unifiedData$All.Received.)
# ggplot(unifiedData, aes(x = All.Received.)) + geom_histogram(stat = "count") + ggtitle("Is all messages received?")

# IS all message receivid, group by number of shard
# ggplot(unifiedData, aes(x = factor(Number.of.Shards), fill = All.Received.)) + geom_bar(stat = "count")

# Is all messages received if throughput <= limit throughput
# ggplot(subset(unifiedData, Thoughput < 1000 * Number.of.Shards), aes(x = factor(Number.of.Shards), fill = All.Received.)) + geom_bar(stat = "count")

# Is there any error (IN PRODUCER)
table(unifiedData$Errors)
# ggplot(unifiedData, aes(x = factor(Number.of.Shards), fill = Errors == 0)) + geom_bar(stat = "count")

# Is there any error if thoughput <= limit throuput (IN PRODUCER)
# ggplot(subset(unifiedData, Thoughput < 1000 * Number.of.Shards), aes(x = factor(Number.of.Shards), fill = Errors == 0)) + geom_bar(stat = "count")

# Maxmimum Through put each number of shards that no Error in producer
aggregate(Throughput ~ Number.of.Shards, subset(unifiedData, (Errors == 0)), max)

# Maxmimum Through put each number of shards that all messageds received in consumer
aggregate(Throughput ~ Number.of.Shards, subset(unifiedData, (All.Received.)), max)

# Maxmimum Through put each number of shards that all messageds received in consumer && No Erros in producer
# noErrorAllReceived <- aggregate(Thoughput ~ Number.of.Shards, subset(unifiedData, (All.Received. & (Errors == 0))), max)
# noErrorAllReceived <- merge(noErrorAllReceived, unifiedData, by = c("Number.of.Shards", "Thoughput"))
# noErrorAllReceived <- noErrorAllReceived[order(noErrorAllReceived$Number.of.Shards),]
# noErrorAllReceived


# Plot no error all received by # of shards
moltenThroughputAndThreoticalKinesis <- melt(unifiedData[,c("Number.of.Shards", "Throughput", "Throughput.Limit")], 
                                             id = "Number.of.Shards")
ggplot(moltenThroughputAndThreoticalKinesis , aes(x = factor(Number.of.Shards), y = value, fill = variable)) + 
  geom_bar(stat = "identity", position = position_dodge())

# Plot producer and consumer in all settings
unifiedData$Desired.Rate.In.Millions <- unifiedData$Desired.Rate / 1000000
moltenProducerAndConsumer <- melt(unifiedData[,c("Number.of.Shards", "Throughput", "Throughput.Average", "Desired.Rate.In.Millions")], 
                                  id = c("Number.of.Shards", "Desired.Rate.In.Millions"))
ggplot(moltenProducerAndConsumer, aes(x = factor(Desired.Rate.In.Millions), y = value, fill = variable)) + 
  geom_bar(stat = "identity", position = position_dodge()) + facet_wrap(~factor(Number.of.Shards))

# Throuput vs Latency in all settings: 
# ggplot(subset(unifiedData), aes(x = Latency.Average, y = Throughput, color = All.Received.)) + geom_point()

# Throuput vs Latency in each settings:
ggplot(subset(unifiedData), aes(x = log(Latency.Average), y = log(Throughput))) + geom_point() + facet_wrap(~ Number.of.Shards)

