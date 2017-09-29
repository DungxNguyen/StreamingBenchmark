package kafka.benchmark.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

public class SimpleBenchmarkKafkaProducerRunner {

	public static void main(String[] args)
			throws IOException, URISyntaxException, InterruptedException, ExecutionException {

		KafkaProducerConfiguration config = new KafkaProducerConfiguration();
		config.setConfigName("test-null");
		config.setTopic("topic-null");
		config.setDuration(10);
		config.setRatePerHour(10000);
		config.setGap(-1);
		config.setBlock(10);
		config.setDefaultKafkaProperties();

		SimpleBenchmarkKafkaProducer kafkaBenchmark = new SimpleBenchmarkKafkaProducer(config);
		kafkaBenchmark.execute();
	}

}
