package kinesis.consumer.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkConsumerWorkgroup {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorkgroup.class);

	private int numberOfApplications;

	public BenchmarkConsumerWorkgroup(int numberOfApplications) {
		this.numberOfApplications = numberOfApplications;
	}

	public static void main(String[] args) {
		BenchmarkConsumerWorkgroup workgroup = new BenchmarkConsumerWorkgroup(Integer.valueOf(args[0]));
		workgroup.execute();
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					workgroup.printLatency();
					workgroup.printThroughput();
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	private void printLatency() {
		LOGGER.info("Average Latency: " + BenchmarkConsumerWorker.getAverageLatency());
	}
	
	private void printThroughput() {
		LOGGER.info("Throughput: (KB/s) " + BenchmarkConsumerWorker.getAverageThouput());
	}

	private void execute() {
		for (int i = 0; i < numberOfApplications; i++) {
			BenchmarkConsumerWorker worker = new BenchmarkConsumerWorker("Benchmark" + i);
			Thread workerThread = new Thread(worker);
			workerThread.start();
		}
	}

}
