package kinesis.consumer.benchmark;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import data.genenator.KinesisTestSuite;

public class BenchmarkConsumerWorker implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorker.class);
	private static final int TIME_OUT_AFTER_RECEIVE_CODE = 60;
	private static final String CONSUMER_METRICS_FILENAME = "consumer.csv";

	private static long DURATION_START = System.currentTimeMillis();
	private static List<Long> latency = Collections.synchronizedList(new CopyOnWriteArrayList<Long>());
	private static List<Integer> capacity = Collections.synchronizedList(new CopyOnWriteArrayList<Integer>());
	private static long startingTime;

	private Set<Integer> idSet = Collections.synchronizedSet(new ConcurrentSkipListSet<Integer>());
	private long checkCode = -1;
	private Worker worker;
	private boolean running = false;

	private String applicationName;
	protected static BenchmarkConsumerMetrics metrics = new BenchmarkConsumerMetrics();

	public BenchmarkConsumerWorker(String applicationName) {
		setApplicationName(applicationName);
	}

	public void execute() throws UnknownHostException {
		running = true;
		final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(applicationName,
				KinesisTestSuite.STREAM_NAME, new ProfileCredentialsProvider("default"),
				InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID());
		config.withRegionName("us-west-1");
		final IRecordProcessorFactory recordProcessorFactory = new BenchmarkConsumerRecordProcessorFactory(this);
		worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(config).build();
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

	public synchronized static double getAverageLatency() {
		int size = latency.size();
		double mlatency = (double) latency.stream().mapToLong(Long::longValue).sum() / size;
		latency.clear();
		if (mlatency != 0 && !Double.isNaN(mlatency)) {
			metrics.latency.add(mlatency);
		}
		return mlatency;
	}

	public synchronized static double getAverageThouput() {
		int sumCapacity = capacity.stream().mapToInt(Integer::intValue).sum();
		capacity.clear();
		double throughput = (double) sumCapacity / (System.currentTimeMillis() - getStartingTime());
		startingTime = System.currentTimeMillis();
		if (throughput != 0) {
			metrics.thoughput.add(throughput);
		}
		return throughput;
	}

	public static void setStartingTime() {
		if (startingTime == 0) {
			startingTime = System.currentTimeMillis();
			DURATION_START = startingTime;
			LOGGER.info("DURATION_START: Start receiving records");
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

	public synchronized void addId(int id) {
		if (idSet.contains(id)) {
			LOGGER.info("Duplicated value: 	" + id);
		} else {
			idSet.add(id);
		}
	}

	public void processCheckCode(long code) {
		if (checkCode != -1)
			return;
		checkCode = code;
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(TIME_OUT_AFTER_RECEIVE_CODE * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					shutdown();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}).start();
	}

	public boolean checkCode() {
		if (checkCode == -1L)
			return false;
		for (int i = 0; i <= checkCode; i++)
			if (!idSet.contains(i))
				return false;
		return true;
	}

	public void shutdown() throws IOException {
		LOGGER.info("Try to shutdown, status running: " + running);
		if (!running)
			return;
		worker.shutdown();
		printMeasurement();
		running = false;
	}

	public void printMeasurement() throws IOException {
		if (checkCode()) {
			LOGGER.info("All messages received");
			metrics.allReceived = true;
		} else {
			LOGGER.info("Messeages LOST");
			metrics.allReceived = false;
		}
		metrics.time = (int) (System.currentTimeMillis() - DURATION_START) / 1000;
		metrics.appendToFile(CONSUMER_METRICS_FILENAME);
	}

	public boolean genRunningStatus() {
		return running;
	}
}
