package kafka.benchmark.consumer;

import java.util.Properties;

public class KafkaConsumerConfiguration {
	Properties consumerProperties;
	String topic;

	public Properties getConsumerProperties() {
		return consumerProperties;
	}

	public String getTopic() {
		return topic;
	}

	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setDefaultConsumerProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka.aws:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		setConsumerProperties(props);
	}

}
