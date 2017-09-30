package kafka.common.quickstart;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;


public class SimpleKafkaProducer {

	public static void main(String[] args) {

		System.out.println("Hello Kafka");
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "kafka.aws:9092");
//		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 1);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>("mytopic", Integer.toString(i), Integer.toString(i)));

		 for( Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet() ) {
			System.out.println(entry.getKey() + ": " + entry.getValue().value()); 
		 }
		 producer.close();
	}

}
