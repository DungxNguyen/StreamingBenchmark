package kafka.benchmark.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import generic.producer.ProducerInterface;
import generic.producer.RecordTemplate;

public class KafkaProducerWrapper extends KafkaProducer implements ProducerInterface {
	
	private String kafkaTopic;
	private ObjectMapper objectMapper = new ObjectMapper();
	private Properties kafkaProperties;

	public KafkaProducerWrapper(Properties kafkaProperties) {
		super(kafkaProperties);
		kafkaTopic = kafkaProperties.getProperty("topic", "topic-null");
		this.kafkaProperties = kafkaProperties;
	}

	@Override
	public Future sendMessage(RecordTemplate record) {
		ProducerRecord kafkaRecord = null;
		try {
			kafkaRecord = new ProducerRecord(kafkaTopic, objectMapper.writeValueAsString(record));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return send(kafkaRecord);
	}

	@Override
	public ProducerInterface createProducer() {
		return new KafkaProducerWrapper(kafkaProperties); 
	}

}
