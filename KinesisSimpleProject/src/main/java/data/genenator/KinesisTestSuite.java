package data.genenator;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class KinesisTestSuite {

	private static final long WAITING_TIME = 60000;
	private static final long EXPERIMENT_TIME_OUT = 10 * 60 * 1000; // 10 min * 60s/min * 1000 ms/s
	private static final String SSH_COMMAND = "ssh ec2-user@";
	private static final String PRODUCER_IP = "54.183.247.57";
	private static final String CONSUMER_IP = "54.183.246.114";
	private static final String CONSUMER_COMMAND = " java -cp hello-1.0.0.jar kinesis.consumer.benchmark.BenchmarkConsumerWorkgroup";
	private static final String PRODUCER_COMMAND = " java -cp hello-1.0.0.jar data.genenator.DataGeneratorRealTimeTest";
	private static final String CLEAN_COMMAND = " killall java";

	public static void main(String[] args) throws Exception {
		
		process(SSH_COMMAND + CONSUMER_IP + CLEAN_COMMAND);
		process(SSH_COMMAND + PRODUCER_IP + CLEAN_COMMAND);
		
		String consumerParams = " testSuite 1";
		processThread(SSH_COMMAND + CONSUMER_IP + CONSUMER_COMMAND + consumerParams);

		Thread.sleep(WAITING_TIME);

		String producerParams = " testSuite 36000 10";
		processThread(SSH_COMMAND + PRODUCER_IP + PRODUCER_COMMAND + producerParams);
	}

	public static void processThread(String command) {
		new Thread(new Runnable() {
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
		}).start();
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
