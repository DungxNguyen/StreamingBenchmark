package kafka.benchmark.consumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import generic.consumer.BenchmarkConsumerWorker;
import generic.producer.RecordTemplate;

public class SimpleBenchmarkKafkaConsumer extends BenchmarkConsumerWorker {
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleBenchmarkKafkaConsumer.class);
	private KafkaConsumerConfiguration config;
	private KafkaConsumer<String, byte[]> consumer;
	private int numberOfPartitions;
	KafkaConsumerThread[] kafkaConsumerThreads;

	// public SimpleBenchmarkKafkaConsumer() {
	// }

	public SimpleBenchmarkKafkaConsumer(KafkaConsumerConfiguration config) {
		this.config = config;
		consumer = new KafkaConsumer<>(config.getConsumerProperties());
		consumer.subscribe(Arrays.asList(config.getTopic()));
		numberOfPartitions = consumer.partitionsFor(config.getTopic()).size();
		kafkaConsumerThreads = new KafkaConsumerThread[numberOfPartitions];
		LOGGER.info("Number of partitions: " + numberOfPartitions);
		LOGGER.info("Partition Info:" + consumer.partitionsFor(config.getTopic()).get(0));
	}

	@Override
	public BenchmarkConsumerWorker createConsumer(String identifier) {
		return new SimpleBenchmarkKafkaConsumer(config);
	}

	@Override
	public void execute() throws Exception {
		setRunningStatus(true);
		kafkaConsumerThreads[0] = new KafkaConsumerThread(this.consumer);
		for (int i = 1; i < numberOfPartitions; i++) {
			kafkaConsumerThreads[i] = new KafkaConsumerThread(this.config.getConsumerProperties());
		}
		for (KafkaConsumerThread thread : kafkaConsumerThreads) {
			new Thread(thread).start();
		}
	}

	@Override
	public void consumerShutdown() throws Exception {
		LOGGER.info("Consumer shutdown");
		for (KafkaConsumerThread thread : kafkaConsumerThreads) {
			thread.shutdown();
		}
	}

	class KafkaConsumerThread implements Runnable {
		private KafkaConsumer<String, byte[]> consumerThread;
		private boolean start = false;
		private boolean forceShutdown = false;

		public KafkaConsumerThread(Properties kafkaConsumerProperties) {
			consumerThread = new KafkaConsumer<>(kafkaConsumerProperties);
			consumerThread.subscribe(Arrays.asList(config.getTopic()));
		}

		public KafkaConsumerThread(KafkaConsumer<String, byte[]> consumerThread) {
			this.consumerThread = consumerThread;
		}

		public void shutdown() {
			forceShutdown = true;
			LOGGER.info("Thread shutdown");
		}

		public void stopConsumerThread() {
			consumerThread.close(5, TimeUnit.SECONDS);
		}

		@Override
		public void run() {
			ObjectMapper objectMapper = new ObjectMapper();
			while (!forceShutdown) {
				ConsumerRecords<String, byte[]> records = consumerThread.poll(100);
				for (ConsumerRecord<String, byte[]> record : records) {
					// System.out.printf("offset = %d, key = %s, value length = %s%n",
					// record.offset(), record.key(),
					// record.value().length);
					try {
						byte[] data = record.value();
						RecordTemplate recordTemplate = objectMapper.readValue(data, RecordTemplate.class);
						LOGGER.info("" + recordTemplate.getId());
						addCapacity(data.length);
						if (!start) {
							BenchmarkConsumerWorker.setStartingTime();
							start = true;
						}
						if (recordTemplate.getCat().equals("CHECKCODE")) {
							LOGGER.info("Receive CheckCode: " + recordTemplate.getMsg());
							processCheckCode(Long.valueOf(recordTemplate.getMsg()));
						} else {
							addId(recordTemplate.getId());
							addLatency(System.currentTimeMillis() - recordTemplate.getTime());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			stopConsumerThread();
		}
	}
}
