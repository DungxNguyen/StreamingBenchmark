package generic.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class BenchmarkConsumerMetrics {
	String experimentName;
	int time;
	int numberOfApplications;
	List<Double> latency = new ArrayList<Double>();
	List<Double> thoughput = new ArrayList<Double>();
	boolean allReceived;

	public void appendToFile(String fileName) throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
			PrintWriter mPrintStream = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			mPrintStream.println(
					"Experiment Name, Number of Applications, Duration, All Received?, Latency Average, Latency SD, Latency Max, Latency Min, "
							+ "Thoughput Average, Thoughput SD, Thoughput Max, Throughput Min");
			printMetric(mPrintStream);
			mPrintStream.close();
		} else {
			PrintWriter mPrintStream = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			printMetric(mPrintStream);
			mPrintStream.close();
		}

	}

	private void printMetric(PrintWriter mPrintWriter) {
		StringBuilder str = new StringBuilder();
		str.append(experimentName + ", ");
		str.append(numberOfApplications + ", ");
		str.append(time + ", ");
		str.append(allReceived + ", ");
		str.append(mean(latency) + ", ");
		str.append(sd(latency) + ", ");
		str.append(max(latency) + ", ");
		str.append(min(latency) + ", ");
		str.append(mean(thoughput) + ", ");
		str.append(sd(thoughput) + ", ");
		str.append(max(thoughput) + ", ");
		str.append(min(thoughput));
		mPrintWriter.println(str);
	}

	private double mean(List<Double> list) {
		double m = 0.0;
		for (double d : list)
			m += d;
		return m / list.size();
	}

	private double sd(List<Double> list) {
		if (list.size() < 2) {
			return 0.0;
		}
		double m = mean(list);
		double sd = 0.0;
		for (double d : list)
			sd += Math.pow(d - m, 2);
		return Math.sqrt(sd / (list.size() - 1));
	}

	private double min(List<Double> list) {
		if (list.size() == 0)
			return 0;
		double m = list.get(0);
		for (double d : list)
			if (d < m)
				m = d;
		return m;
	}

	private double max(List<Double> list) {
		if (list.size() == 0)
			return 0;
		double m = list.get(0);
		for (double d : list)
			if (d > m)
				m = d;
		return m;
	}
}
