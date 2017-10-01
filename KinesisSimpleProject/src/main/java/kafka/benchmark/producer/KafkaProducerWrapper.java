package kafka.benchmark.producer;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import generic.producer.ProducerInterface;
import generic.producer.ProducerMetric;
import generic.producer.RecordTemplate;

public class KafkaProducerWrapper extends KafkaProducer implements ProducerInterface {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWrapper.class);

	private String kafkaTopic;
	private ObjectMapper objectMapper = new ObjectMapper();
	private Properties kafkaProperties;

	public KafkaProducerWrapper(Properties kafkaProperties) {
		super(kafkaProperties);
		kafkaTopic = kafkaProperties.getProperty("topic", "topic-null");
		this.kafkaProperties = kafkaProperties;
		LOGGER.info("Producer Topic: " + kafkaTopic);
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
			if (entry.getKey().name().equals("outgoing-byte-rate") && entry.getKey().group().equals("producer-metrics")) {
				producerMetric.dataPerSecond = entry.getValue().value();
			}
			if (entry.getKey().name().equals("record-send-rate")) {
				producerMetric.recordsPerSecond = entry.getValue().value();
				producerMetric.recordsPerHour = producerMetric.recordsPerSecond * 3600;
			}
			if (entry.getKey().name().equals("record-retry-rate"))
				producerMetric.retriesPerRecord = (int) entry.getValue().value();
			if (entry.getKey().name().equals("record-error-rate"))
				producerMetric.error = (int) entry.getValue().value();
		}
		return producerMetric;
	}

}
