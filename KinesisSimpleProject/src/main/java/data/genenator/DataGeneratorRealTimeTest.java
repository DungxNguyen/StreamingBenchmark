package data.genenator;

import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGeneratorRealTimeTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorRealTimeTest.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Data Generator Started");
		DataGeneratorConfiguration config = new DataGeneratorConfiguration();
		config.setRatePerHour(Integer.valueOf(args[0])); // 3600 per hour = 1 per second
		config.setDuration(Integer.valueOf(args[1])); // 60 seconds 
		config.setStartTime(0);
		DataGenerator mDataGenerator = new DataGenerator(config);
		mDataGenerator.executeKinesis();
		LOGGER.info("Data Generator Finished");
	}
}
