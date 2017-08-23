package data.genenator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGeneratorParallelRealTimeTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorParallelRealTimeTest.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Data Generator Started");
		DataGeneratorConfiguration config = new DataGeneratorConfiguration();
		config.setConfigName(args[0]);
		config.setRatePerHour(Integer.valueOf(args[1])); // 3600 per hour = 1 per second
		config.setDuration(Integer.valueOf(args[2])); // 60 seconds
		config.setStartTime(0);
		config.setGap(Integer.valueOf(args[3]));
		config.setBlock(Integer.valueOf(args[4]));
		DataGeneratorParallel mDataGenerator = new DataGeneratorParallel(config);
		mDataGenerator.executeKinesis();
		LOGGER.info("Data Generator Finished");
	}
}
