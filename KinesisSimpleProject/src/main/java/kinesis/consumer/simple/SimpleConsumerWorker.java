package kinesis.consumer.simple;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class SimpleConsumerWorker {
	
	private static Map<String, Integer> logCount = new ConcurrentHashMap<String, Integer>();

	public static void main(String[] args) throws UnknownHostException {
		final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration("SimpleConsumer", "CoffeeStream",
				new ProfileCredentialsProvider("default"),
				InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID());
		config.withRegionName("us-west-1");
		final IRecordProcessorFactory recordProcessorFactory = new SimpleConsumerRecordProcessorFactory();
		final Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(config)
				.build();
		worker.run();
	}
	
	public static void count(String logType, int count){
		if (logCount.containsKey(logType)){
			logCount.put(logType, logCount.get(logType) + count);
			return;
		}
		logCount.put(logType, count);
	}
}
