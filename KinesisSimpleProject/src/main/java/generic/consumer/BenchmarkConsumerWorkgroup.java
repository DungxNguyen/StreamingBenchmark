package generic.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkConsumerWorkgroup {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorkgroup.class);

	private int numberOfApplications;
	private BenchmarkConsumerWorker[] workers;

	public BenchmarkConsumerWorkgroup(String experimentName, int numberOfApplications, BenchmarkConsumerWorker consumer) {
		this.numberOfApplications = numberOfApplications;
		workers = new BenchmarkConsumerWorker[numberOfApplications];
		workers[0] = consumer;
		for (int i = 1; i < numberOfApplications; i++) {
			workers[i] = workers[0].createConsumer("Benchmark" + i);
		}
		BenchmarkConsumerWorker.metrics.numberOfApplications = numberOfApplications;
		BenchmarkConsumerWorker.metrics.experimentName = experimentName;
	}

	public static void main(String[] args) {
		// example of how to run
//		BenchmarkConsumerWorkgroup workgroup = new BenchmarkConsumerWorkgroup(args[0], Integer.valueOf(args[1]), null);
//		workgroup.execute();
	}
	
	private void printLatency() {
		LOGGER.info("Average Latency: " + BenchmarkConsumerWorker.getAverageLatency());
	}

	private void printThroughput() {
		LOGGER.info("Throughput: (KB/s) " + BenchmarkConsumerWorker.getAverageThouput());
	}

	private boolean checkComplete() {
		boolean check = true;
		for (int i = 0; i < numberOfApplications; i++) {
			if (!workers[i].getRunningStatus()) {
				if (workers[i].checkCode())
					LOGGER.info("Application " + i + " check code successfully");
				else
					LOGGER.info("Application " + i + " check code failed.");
			} else {
				check = false;
			}
		}
		return check;
	}

	public void execute() {
		for (int i = 0; i < numberOfApplications; i++) {
			new Thread(workers[i]).start();
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					printLatency();
					printThroughput();
					if (checkComplete())
						break;
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

}
