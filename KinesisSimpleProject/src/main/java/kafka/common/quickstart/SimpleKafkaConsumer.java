package kafka.common.quickstart;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleKafkaConsumer {

	public static void main(String[] args) {
	     Properties props = new Properties();
	     props.put("bootstrap.servers", "kafka.aws:9092");
	     props.put("group.id", "SimpleConsumer");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	     KafkaConsumer consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("topic-null"));
	     while (true) {
	         ConsumerRecords<String, byte[]> records = consumer.poll(100);
//	         System.out.println("Hello");
	         for (ConsumerRecord<String, byte[]> record : records) {
	             System.out.printf("topic = %s, offset = %d, key = %s, value.length = %d%n", record.topic(), record.offset(), record.key(), record.value().length);
	         }	
	     }
	}

}
