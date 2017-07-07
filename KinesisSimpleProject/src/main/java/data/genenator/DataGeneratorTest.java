package data.genenator;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGeneratorTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorTest.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Data Generator Started");
		DataGeneratorConfiguration config = new DataGeneratorConfiguration();
		config.setRatePerHour(10000);
		config.setDuration(3600 * 100); // 100 hours x 3600 sec/hour
		config.setStartTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-01-01 00:00:00").getTime());
		DataGenerator mDataGenerator = new DataGenerator(config);
		mDataGenerator.execute();
		LOGGER.info("Data Generator Finished");
	}

}
