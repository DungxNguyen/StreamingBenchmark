package generic.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkConsumerWorkgroup {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerWorkgroup.class);

	private int numberOfApplications;
	private BenchmarkConsumerWorker[] workers;
	private boolean isRunning = false;

	public BenchmarkConsumerWorkgroup(String experimentName, int numberOfApplications,
			BenchmarkConsumerWorker consumer) {
		this.numberOfApplications = numberOfApplications;
		workers = new BenchmarkConsumerWorker[numberOfApplications];

		for (int i = 0; i < numberOfApplications; i++) {
			if (i == 0) {
				workers[i] = consumer;
			} else {
				workers[i] = consumer.createConsumer(null);
			}
			workers[i].metrics.numberOfApplications = numberOfApplications;
			workers[i].metrics.experimentName = experimentName + "-" + i;
		}
	}

	public static void main(String[] args) {
		// example of how to run
		// BenchmarkConsumerWorkgroup workgroup = new
		// BenchmarkConsumerWorkgroup(args[0], Integer.valueOf(args[1]), null);
		// workgroup.execute();
	}

	// ISSUE: Those are very important: 
	/// Print latency and print throughput, call get average
	// by calling them, worker will calculate current metric and add them to final report
	// if not called, workers will not calucalte until the end
	// TODO: Make sure workers will calculate independently
	private void printLatency() {
		for (int i = 0; i < numberOfApplications; i++) {
			LOGGER.info("Average Latency worker " + i + ": " + workers[i].getAverageLatency());
		}
	}

	private void printThroughput() {
		for (int i = 0; i < numberOfApplications; i++) {
			LOGGER.info("Average Throughput worker " + i + ": " + workers[i].getAverageThroughput());
		}
	}

	private boolean checkComplete() {
		boolean check = true;
		for (int i = 0; i < numberOfApplications; i++) {
			if (!workers[i].getRunningStatus()) {
//				LOGGER.info("Worker " + i + ": " + workers[i].getRunningStatus());
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

	public void execute() throws InterruptedException {
		setRunning(true);
		Thread[] threads = new Thread[numberOfApplications];
		for (int i = 0; i < numberOfApplications; i++) {
			threads[i] = new Thread(workers[i]);
			threads[i].start();
		}

		Thread guard = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					printLatency();
					printThroughput();
					if (checkComplete()) {
						setRunning(false);
						break;
					}
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		guard.start();
//		for (int i = 0; i < numberOfApplications; i++) {
//			threads[i].join();
//		}
//		guard.join();
	}

	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

}
