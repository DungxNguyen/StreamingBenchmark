package data.genenator;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class KinesisTestSuite {

	private static final long WAITING_TIME = 60000;
	private static final long EXPERIMENT_TIME_OUT = 10 * 60 * 1000; // 10 min * 60s/min * 1000 ms/s
	private static final String SSH_COMMAND = "ssh ec2-user@";
	private static final String PRODUCER_IP = "54.183.247.57";
	private static final String CONSUMER_IP = "54.183.248.111";
	private static final String CONSUMER_COMMAND = " java -cp hello-1.0.0.jar kinesis.consumer.benchmark.BenchmarkConsumerWorkgroup";
	private static final String PRODUCER_COMMAND = " java -cp hello-1.0.0.jar data.genenator.DataGeneratorRealTimeTest";
	private static final String CLEAN_COMMAND = " killall java";
	private static final int PRODUCER_DURATION = 60;
	private static final int NUMBER_OF_SHARDS = 32;
	private static final int NUMBER_OF_CONSUMER_APPLICATION = 1;
	private static final int MAX_RATE_PER_SHARD = 1000000;

	public static void main(String args[]) throws InterruptedException {
		for (int i = 1; i <= 10; i++) {
			int desiredRate = MAX_RATE_PER_SHARD * NUMBER_OF_SHARDS * i / 10;

			StringBuilder consumerParams = new StringBuilder();// " testSuite 1";
			StringBuilder producerParams = new StringBuilder();// " testSuite 36000 10";
			String experimentNames = NUMBER_OF_SHARDS + "-" + NUMBER_OF_CONSUMER_APPLICATION + "-" + desiredRate;

			consumerParams.append(" ");
			consumerParams.append(experimentNames);
			consumerParams.append(" ");
			consumerParams.append(NUMBER_OF_CONSUMER_APPLICATION);

			producerParams.append(" ");
			producerParams.append(experimentNames + " ");
			producerParams.append(desiredRate + " ");
			producerParams.append(PRODUCER_DURATION);

			experiment(consumerParams.toString(), producerParams.toString());
		}
	}

	public static void experiment(String consumerParams, String producerParams) throws InterruptedException {

		process(SSH_COMMAND + CONSUMER_IP + CLEAN_COMMAND);
		process(SSH_COMMAND + PRODUCER_IP + CLEAN_COMMAND);

		// Init consumer and producer threads
		Thread consumerThread = processThread(SSH_COMMAND + CONSUMER_IP + CONSUMER_COMMAND + consumerParams);
		Thread producerThread = processThread(SSH_COMMAND + PRODUCER_IP + PRODUCER_COMMAND + producerParams);

		// Start guard to limit time of experiment
		Thread guard = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(EXPERIMENT_TIME_OUT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				process(SSH_COMMAND + CONSUMER_IP + CLEAN_COMMAND);
				process(SSH_COMMAND + PRODUCER_IP + CLEAN_COMMAND);
			}
		});
		guard.start();

		// Start consumer
		consumerThread.start();

		// Wait for consumer ready
		Thread.sleep(WAITING_TIME);

		// Start producer
		producerThread.start();

		producerThread.join();
		consumerThread.join();
		guard.interrupt();
	}

	public static Thread processThread(String command) {
		return new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Process p = Runtime.getRuntime().exec(command);
					BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
					String line = null;
					while ((line = stderr.readLine()) != null) {
						System.err.println(line);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static void process(String command) {
		try {
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line = null;
			while ((line = stderr.readLine()) != null) {
				System.err.println(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
