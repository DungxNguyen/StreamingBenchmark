package kinesis.consumer.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkConsumerWorkgroup {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorkgroup.class);

	private int numberOfApplications;
	private BenchmarkConsumerWorker[] workers;

	public BenchmarkConsumerWorkgroup(String experimentName, int numberOfApplications) {
		this.numberOfApplications = numberOfApplications;
		workers = new BenchmarkConsumerWorker[numberOfApplications];
		for (int i = 0; i < numberOfApplications; i++) {
			workers[i] = new BenchmarkConsumerWorker("Benchmark" + i);
		}
		BenchmarkConsumerWorker.metrics.numberOfApplications = numberOfApplications;
		BenchmarkConsumerWorker.metrics.experimentName = experimentName;
	}

	public static void main(String[] args) {
		BenchmarkConsumerWorkgroup workgroup = new BenchmarkConsumerWorkgroup(args[0], Integer.valueOf(args[1]));
		workgroup.execute();
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					workgroup.printLatency();
					workgroup.printThroughput();
					if (workgroup.checkComplete())
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

	private void printLatency() {
		LOGGER.info("Average Latency: " + BenchmarkConsumerWorker.getAverageLatency());
	}

	private void printThroughput() {
		LOGGER.info("Throughput: (KB/s) " + BenchmarkConsumerWorker.getAverageThouput());
	}

	private boolean checkComplete() {
		boolean check = true;
		for (int i = 0; i < numberOfApplications; i++) {
			// if (workers[i].checkCode()) {
			// LOGGER.info("Application " + i + " check code successfully");
			// } else if (!workers[i].genRunningStatus()){
			// LOGGER.info("Application " + i + " check code failed.");
			// } else {
			// check = false;
			// }
			if (!workers[i].genRunningStatus()) {
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

	private void execute() {
		for (int i = 0; i < numberOfApplications; i++) {
			Thread workerThread = new Thread(workers[i]);
			workerThread.start();
		}
	}

}
