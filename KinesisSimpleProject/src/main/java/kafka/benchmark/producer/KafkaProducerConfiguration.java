package kafka.benchmark.producer;

import java.util.Properties;

import generic.producer.BenchmarkProducerConfiguration;

public class KafkaProducerConfiguration extends BenchmarkProducerConfiguration {
	private String topic;
	private Properties kafkaProperties;

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
		kafkaProperties.put("topic", topic);
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
	
	public void setKafkaProperties(Properties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	public void setDefaultKafkaProperties() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "kafka.aws:9092");
		kafkaProperties.put("acks", "all");
		kafkaProperties.put("retries", Integer.MAX_VALUE);
		
		kafkaProperties.put("batch.size", 16384);
		kafkaProperties.put("linger.ms", 1);
		kafkaProperties.put("buffer.memory", 33554432);
		kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		// add Kafka Properties to configuration
		this.setKafkaProperties(kafkaProperties);
		
	}

}
