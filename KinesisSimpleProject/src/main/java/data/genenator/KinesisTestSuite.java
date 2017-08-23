package data.genenator;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import kinesis.stream.manager.KinesisStreamUtils;

public class KinesisTestSuite {

	public static final String STREAM_NAME = "CoffeeStream2";
	private static final long WAITING_TIME = 60000;
	private static final long EXPERIMENT_TIME_OUT = 200 * 60 * 1000; // 200 min * 60s/min * 1000 ms/s
	private static final String SSH_COMMAND = "ssh ec2-user@";
	private static final String PRODUCER_IP = "34.228.208.214";
	private static final String CONSUMER_IP = "34.229.224.28";
	private static final String CONSUMER_COMMAND = " java -cp hello-1.0.0.jar kinesis.consumer.benchmark.BenchmarkConsumerWorkgroup";
	private static final String PRODUCER_COMMAND = " java -cp hello-1.0.0.jar data.genenator.DataGeneratorRealTimeTest";
	private static final String CLEAN_COMMAND = " killall java";
	private static final int PRODUCER_DURATION = 60;
	private static final int NUMBER_OF_MAX_SHARDS = 32;
	private static final int[] EXPERIMENTS = new int[] { 1, 2, 4, 8, 16, 32 };
	// }; // Full throughput, 1/2, 1/4, 1/8, ...
	// private static final int[] EXPERIMENTS = new int[] { 1 };
	private static final int[] LIST_SHARDS = new int[] { 32, 16, 8, 4, 2, 1 }; // 1 shard, 2, 4, 8, 16
	private static final int NUMBER_OF_CONSUMER_APPLICATION = 1;
	private static final int MAX_RATE_PER_SHARD = 1000000;
	// private static final int NUMBER_OF_EXPERIMENT = 1;
	private static final boolean TEST_PRODUCER_ONLY = false;

	public static void main(String args[]) throws InterruptedException {
		// for (int i = NUMBER_OF_EXPERIMENT; i >= 1; i--) {
		// int desiredRate = MAX_RATE_PER_SHARD * NUMBER_OF_SHARDS * i /
		// NUMBER_OF_EXPERIMENT;
		for (int shards : LIST_SHARDS) {
			KinesisStreamUtils.deleteStream(STREAM_NAME, 300000);
			KinesisStreamUtils.createStream(STREAM_NAME, shards, 300000);

			for (int exp : EXPERIMENTS) {
				int desiredRate = MAX_RATE_PER_SHARD * NUMBER_OF_MAX_SHARDS / exp;

				int gap = -1;
				int block = 100;

				StringBuilder consumerParams = new StringBuilder();// " testSuite 1";
				StringBuilder producerParams = new StringBuilder();// " testSuite 36000 10";
				String experimentNames = args[0] + "-" + shards + "-" + NUMBER_OF_CONSUMER_APPLICATION + "-"
						+ desiredRate + "-" + gap + "-" + block;

				consumerParams.append(" ");
				consumerParams.append(experimentNames);
				consumerParams.append(" ");
				consumerParams.append(NUMBER_OF_CONSUMER_APPLICATION);

				producerParams.append(" ");
				producerParams.append(experimentNames + " ");
				producerParams.append(desiredRate + " ");
				producerParams.append(PRODUCER_DURATION + " ");
				producerParams.append(gap + " ");
				producerParams.append(block + " ");

				experiment(consumerParams.toString(), producerParams.toString());
			}

		}
		process("rsync -prvz ec2-user@" + PRODUCER_IP + ":producer.csv /home/dnguyen/Dropbox/producer.csv");
		process("rsync -prvz ec2-user@" + CONSUMER_IP + ":consumer.csv /home/dnguyen/Dropbox/consumer.csv");
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

		if (!TEST_PRODUCER_ONLY) {
			// Start consumer
			consumerThread.start();

			// Wait for consumer ready
			Thread.sleep(WAITING_TIME);
		}

		// Start producer
		producerThread.start();

		// Wait for consumer and producer to finish
		producerThread.join();

		if (!TEST_PRODUCER_ONLY) {
			consumerThread.join();
		}

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
