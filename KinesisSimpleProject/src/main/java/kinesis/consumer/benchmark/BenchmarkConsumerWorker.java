package kinesis.consumer.benchmark;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class BenchmarkConsumerWorker implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorker.class);

	private static List<Long> latency = Collections.synchronizedList(new CopyOnWriteArrayList<Long>());
	private static List<Integer> capacity = Collections.synchronizedList(new CopyOnWriteArrayList<Integer>());
	private static long startingTime;

	private String applicationName;

	public BenchmarkConsumerWorker(String applicationName) {
		setApplicationName(applicationName);
	}

	public void execute() throws UnknownHostException {
		final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(applicationName, "CoffeeStream",
				new ProfileCredentialsProvider("default"),
				InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID());
		config.withRegionName("us-west-1");
		final IRecordProcessorFactory recordProcessorFactory = new BenchmarkConsumerRecordProcessorFactory();
		final Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(config)
				.build();
		worker.run();
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		if (applicationName.equalsIgnoreCase("")) {
			this.applicationName = applicationName;
		} else {
			this.applicationName = UUID.randomUUID().toString();
		}
	}

	public static void addLatency(long latency) {
		BenchmarkConsumerWorker.latency.add(latency);
	}

	public static void addCapacity(int capacity) {
		BenchmarkConsumerWorker.capacity.add(capacity);
	}

	public static double getAverageLatency() {
		int size = latency.size();
		long sumLatency = latency.stream().mapToLong(Long::longValue).sum();
		latency.clear();
		return (double) sumLatency / size;
	}

	public static double getAverageThouput() {
		int sumCapacity = capacity.stream().mapToInt(Integer::intValue).sum();
		capacity.clear();

		double throughput = (double) sumCapacity / (System.currentTimeMillis() - getStartingTime());

		startingTime = System.currentTimeMillis();
		return throughput;
	}

	public static void setStartingTime() {
		if (startingTime == 0) {
			startingTime = System.currentTimeMillis();
		}
	}

	public static long getStartingTime() {
		return startingTime;
	}

	@Override
	public void run() {
		try {
			execute();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
