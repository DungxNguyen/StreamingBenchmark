package kafka.benchmark.consumer;

import generic.consumer.BenchmarkConsumerWorker;
import generic.consumer.BenchmarkConsumerWorkgroup;

public class SimpleBenchmarkKafkaConsumerRunner {

	public static void main(String[] args) throws InterruptedException {
		execute();
	}

	private static void execute() throws InterruptedException {
		// Create config for the kafka worker
		KafkaConsumerConfiguration config = new KafkaConsumerConfiguration();
		config.setDefaultConsumerProperties();
		config.setTopic("topic-null");
		
		// Create a worker, which is equivalent with 1 application, or 1
		BenchmarkConsumerWorker kafkaConsumerWorker = new SimpleBenchmarkKafkaConsumer(config);

		// CReate a workgroup, that has multiple applications
		BenchmarkConsumerWorkgroup workgroup = new BenchmarkConsumerWorkgroup("experimentName-null", 1, kafkaConsumerWorker);
		workgroup.execute();
	}
}
