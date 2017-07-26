package data.genenator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class KinesisMetric {
	public String experimentName;
	public int desiredRate;
	public int recordNumber;
	public int error;
	public double bufferingTime;
	public double retriesPerRecord;
	public double recordsPerSecond;
	public double recordsPerHour;
	public double dataPerSecond;

	public void appendToFile(String fileName) throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
			PrintWriter mPrintStream = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			mPrintStream.println(
					"Experiment Name, Desired Rate, Records, Errors, Buffering Time, Retries Per Second, Records Per Second, Records Per Hour, Thoughput");
			mPrintStream.println(experimentName + ", " + desiredRate + ", " + recordNumber + ", " + error + ", " + bufferingTime + ", "
					+ retriesPerRecord + ", " + recordsPerSecond + ", " + recordsPerHour + ", " + dataPerSecond);
			mPrintStream.close();
		} else {
			PrintWriter mPrintStream = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			mPrintStream.println(experimentName + ", " + desiredRate + ", " + recordNumber + ", " + error + ", " + bufferingTime + ", "
					+ retriesPerRecord + ", " + recordsPerSecond + ", " + recordsPerHour + ", " + dataPerSecond);
			mPrintStream.close();
		}
	}
}
