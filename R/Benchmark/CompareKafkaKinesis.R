library(ggplot2)
library(reshape2)

# Kafka Data
kafkaConsumerHigh <- "consumer-high.csv"
kafkaProducerHigh <- "producer-high.csv"
kafkaConsumerMid <- "consumer-mid.csv"
kafkaProducerMid <- "producer-mid.csv"
kafkaConsumerLow <- "consumer-low.csv"
kafkaProducerLow <- "producer-low.csv"

kafkaConsumerHighRaw <- read.csv(kafkaConsumerHigh, header = TRUE, sep = ",")
kafkaConsumerMidRaw <- read.csv(kafkaConsumerMid, header = TRUE, sep = ",")
kafkaConsumerLowRaw <- read.csv(kafkaConsumerLow, header = TRUE, sep = ",")
kafkaProducerHighRaw <- read.csv(kafkaProducerHigh, header = TRUE, sep = ",")
kafkaProducerMidRaw <- read.csv(kafkaProducerMid, header = TRUE, sep = ",")
kafkaProducerLowRaw <- read.csv(kafkaProducerLow, header = TRUE, sep = ",")

# Add type
kafkaConsumerHighRaw$Type <- "kafka-high"
kafkaConsumerMidRaw $Type <- "kafka-mid"
kafkaConsumerLowRaw $Type <- "kafka-low"
kafkaProducerHighRaw$Type <- "kafka-high"
kafkaProducerMidRaw $Type <- "kafka-mid"
kafkaProducerLowRaw $Type <- "kafka-low"

# Aggregate kafka data
kafkaConsumer <- rbind(kafkaConsumerHighRaw, kafkaConsumerMidRaw, kafkaConsumerLowRaw)
kafkaProducer <- rbind(kafkaProducerHighRaw, kafkaProducerMidRaw, kafkaProducerLowRaw)
rm(kafkaConsumerHighRaw, kafkaConsumerLowRaw, kafkaConsumerMidRaw)
rm(kafkaProducerHighRaw, kafkaProducerLowRaw, kafkaProducerMidRaw)

# Normalize Experiment Name
kafkaConsumer$Experiment.Name <- sapply(kafkaConsumer$Experiment.Name, function(x) paste(strsplit(as.character(x), split = "-", fixed = TRUE)[[1]][1:4], collapse = "-"))

# Kinesis data
kinesisConsumer <- "consumer.csv"
kinesisProducer <- "producer.csv"
kinesisConsumerRaw <- read.csv(kinesisConsumer, header = TRUE, sep = ",")
kinesisProducerRaw <- read.csv(kinesisProducer, header = TRUE, sep = ",")

# Kinesis Dummy Values
kinesisProducerRaw$Duration <- NA
kinesisProducerRaw$Estimated.Records.Per.Second <- NA

# Type
kinesisProducerRaw$Type <- "kinesis"
kinesisConsumerRaw$Type <- "kinesis"

# Aggregate kafka and kinesis
consumer <- rbind(kafkaConsumer, kinesisConsumerRaw)
producer <- rbind(kafkaProducer, kinesisProducerRaw)

rm(kafkaConsumer, kafkaProducer)
rm(kinesisConsumerRaw, kinesisProducerRaw)

# Merge tables
unifiedData <- merge(consumer, producer, 
                     by = c("Experiment.Name", "Type"))
unifiedData$Type <- factor(unifiedData$Type, levels(factor(unifiedData$Type))[c(4, 1, 3, 2)])
  # levels(factor(unifiedData$Type))
rm(consumer, producer)


write.csv2(unifiedData, "experiment.csv", sep = ",")
############################################################################
# Number of shards
unifiedData$Number.of.Shards <- apply(unifiedData, 1, function(x) {
  if (x["Type"] %in% c("kafka-high", "kafka-mid", "kafka-low")){
    strsplit(as.character(x["Experiment.Name"]), split = "-", fixed =  TRUE)[[1]][4]
  }
  else{
    strsplit(as.character(x["Experiment.Name"]), split = "-", fixed =  TRUE)[[1]][2]
  }
}
)
unifiedData$Number.of.Shards <- as.numeric(unifiedData$Number.of.Shards)
unifiedData$Throughput.Limit <- unifiedData$Number.of.Shards * 1024
unifiedData$ProducerThroughput <- apply(unifiedData, 1, function(x) {
  if (x["Type"] %in% c("kinesis")){
    as.numeric(x["Throughput"])
  }
  else{
    as.numeric(x["Throughput"]) / 1024
  }
}
)
unifiedData$Desired.Rate.In.Millions <- unifiedData$Desired.Rate / 1000000

# Plot maximum throughput of producers
moltenProducerThroughput <- melt(unifiedData[,c("Number.of.Shards", "ProducerThroughput", "Type")], 
                                             id = c("Number.of.Shards", "Type"))
moltenProducerThroughput <- aggregate(value ~ Number.of.Shards + Type + variable, data = moltenProducerThroughput, FUN = max)
plot <- ggplot(moltenProducerThroughput , aes(x = factor(Number.of.Shards), y = value, fill = Type)) + 
  geom_bar(stat = "identity", position = position_dodge())
plot <- plot + xlab("Count Of Shards/Partitions") +
               ylab("Maximum Producer Throughput (KBps)")
plot
ggsave("MaximumProducerThroughputVSShards.png", plot = plot, device = "png")

# Plot 
moltenProducerAndConsumer <- unifiedData[,c("Number.of.Shards", "ProducerThroughput", "Throughput.Average", "Desired.Rate.In.Millions", "Type")]
names(moltenProducerAndConsumer)[2] <- "Producer "
names(moltenProducerAndConsumer)[3] <- "Consumer "
moltenProducerAndConsumer <- melt(moltenProducerAndConsumer, 
                                  id = c("Number.of.Shards", "Desired.Rate.In.Millions", "Type"))
names(moltenProducerAndConsumer)[4] <- "Throughput"
plot <- ggplot(moltenProducerAndConsumer, aes(x = factor(Desired.Rate.In.Millions), y = value, fill = Throughput)) + 
  geom_bar(stat = "identity", position = position_dodge()) + facet_grid(Type ~factor(Number.of.Shards)) +
  theme(legend.position = "top")

plot <- plot + xlab("Message Generation Rate (Million Message/Hour)") + ylab("Throughput (KBps)")
plot
ggsave("ProducerVSConsumer.png", plot = plot, device = "png")

# Producer vs Latency
plot <- ggplot(subset(unifiedData), aes(x = log(Latency.Average), y = log(ProducerThroughput))) + geom_point() + facet_grid(Type ~factor(Number.of.Shards))
plot <- plot + xlab("Log Scale of Average Latency (second)") +
               ylab("Log Scale of Producer Throughput (KBps)")
plot
ggsave("ProducerVsLatency.png", plot = plot, device = "png")

# Desired vs Latency
plot <- ggplot(subset(unifiedData), aes(x = log(Latency.Average), y = log(Desired.Rate.In.Millions))) + geom_point() + facet_grid(Type ~factor(Number.of.Shards))
plot <- plot + xlab("Log Scale of Average Latency (second)") +
               ylab("Log Scale of Message Generation Rate (Message/Hour)")
plot
ggsave("DesiredVsLatency.png", plot = plot, device = "png")
