package kafka.benchmark.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import generic.producer.BenchmarkProducer;

public class SimpleBenchmarkKafkaProducer extends BenchmarkProducer{

	public SimpleBenchmarkKafkaProducer(KafkaProducerConfiguration config) throws IOException, URISyntaxException {
		super(config);
		initKafkaProducer(config.getKafkaProperties());
	}
	
	private void initKafkaProducer(Properties kafkaProperties) {
		KafkaProducerWrapper kafkaProducer = new KafkaProducerWrapper(kafkaProperties);
		setProducer(kafkaProducer);
	}
	
	
}