package kafka.benchmark.experiment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import generic.consumer.BenchmarkConsumerWorker;
import generic.consumer.BenchmarkConsumerWorkgroup;
import kafka.benchmark.consumer.KafkaConsumerConfiguration;
import kafka.benchmark.consumer.SimpleBenchmarkKafkaConsumer;
import kafka.benchmark.producer.KafkaProducerConfiguration;
import kafka.benchmark.producer.SimpleBenchmarkKafkaProducer;

public class KafkaExperiment {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaExperiment.class);
	private static final int DURATION = 10;

	private static final String[] KAFKA_SERVERS = new String[] { "kafka-01.aws", "kafka-02.aws", "kafka-04.aws",
			"kafka-08.aws", "kafka-16.aws", "kafka-32.aws" };
	private static final String[] EXPERIMENT_SUFFIX = new String[] { "01", "02", "04", "08", "16", "32" };
	private static final int NUM_LEVEL = 6;
	private static final int NUM_SERVER = 6;

	public static void main(String args[])
			throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		execute();
	}

	private static void execute() throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		 for (int level = 0; level < NUM_LEVEL; level++) {
		 for (int server = 0; server < NUM_SERVER; server++) {
				BenchmarkConsumerWorkgroup kafkaConsumer = startConsumer(level, server);
				startProducer(level, server);
				while (kafkaConsumer.isRunning()) {
					LOGGER.info("Producer for this experiment finishes. Consumer is till running: " + level + " - " + server);
					Thread.sleep(10000);
				}
			}
		}
  
//		int level = 5, server = 5;
//		startConsumer(level, server);
//		startProducer(level, server);
	}

	private static BenchmarkConsumerWorkgroup startConsumer(int level, int server) throws InterruptedException {
		// Create config for the kafka worker
		KafkaConsumerConfiguration config = new KafkaConsumerConfiguration();
		config.setDefaultConsumerProperties();
		config.getKafkaProperties().replace("bootstrap.servers", KAFKA_SERVERS[server] + ":9092");
		config.setTopic("experiment-p-" + EXPERIMENT_SUFFIX[server]);

		// Create a worker, which is equivalent with 1 application, or 1
		BenchmarkConsumerWorker kafkaConsumerWorker = new SimpleBenchmarkKafkaConsumer(config);

		// CReate a workgroup, that has multiple applications
		BenchmarkConsumerWorkgroup workgroup = new BenchmarkConsumerWorkgroup(
				1000000 * getLevelRate(level) + "-experiment-p-" + EXPERIMENT_SUFFIX[server], 1, kafkaConsumerWorker);
		workgroup.execute();
		return workgroup;
	}

	private static void startProducer(int level, int server)
			throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		KafkaProducerConfiguration config = new KafkaProducerConfiguration();
		config.setDefaultKafkaProperties();
		config.getKafkaProperties().replace("bootstrap.servers", KAFKA_SERVERS[server] + ":9092");
		config.setConfigName(1000000 * getLevelRate(level) + "-experiment-p-" + EXPERIMENT_SUFFIX[server]);
		config.setTopic("experiment-p-" + EXPERIMENT_SUFFIX[server]);
		config.setDuration(DURATION);
		config.setRatePerHour(1000000 * getLevelRate(level));
		config.setGap(-1);
		config.setBlock(10);

		SimpleBenchmarkKafkaProducer kafkaBenchmark = new SimpleBenchmarkKafkaProducer(config);
		kafkaBenchmark.execute();
	}

	private static int getLevelRate(int level) {
		return (int) Math.pow(2, level);
	}
}
