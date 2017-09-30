package kafka.benchmark.producer;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import generic.producer.ProducerInterface;
import generic.producer.ProducerMetric;
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
			kafkaRecord = new ProducerRecord(kafkaTopic, objectMapper.writeValueAsBytes(record));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return send(kafkaRecord);
	}

	@Override
	public ProducerInterface createProducer() {
		return new KafkaProducerWrapper(kafkaProperties);
	}

	@Override
	public ProducerMetric calculateMetric() {
		ProducerMetric producerMetric = new ProducerMetric();
		Map<MetricName, ? extends Metric> kafkaMetricMap = metrics();
		for (Entry<MetricName, ? extends Metric> entry : kafkaMetricMap.entrySet()) {
			if (entry.getKey().name().equals("outgoing-byte-rate"))
				producerMetric.dataPerSecond = entry.getValue().value();
		}
		return producerMetric;
	}

}
