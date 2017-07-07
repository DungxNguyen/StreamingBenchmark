package data.genenator;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kinesis.common.RecordTemplate;

public class DataGenerator {
	// CONSTANT
	private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);
	private static final String DEFAULT_VALUE = "N/A";
	private static final String FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE = "Field_Value_Combination_Distribution.json";

	private DataGeneratorConfiguration config;
	private ExponentialDistribution mExponentialDistribution;
	private DateFormat mDateFormat;
	private FieldGenerator mFieldGenerator;
	private ObjectMapper mObjectMapper;

	// Environment
	private double averageTimeGapBetween2Records; // in milliseconds

	// Statistics
	private int recordCounter;
	private int realRate;
	private Map<String, Integer> combinationCount;

	public DataGenerator(DataGeneratorConfiguration config) throws IOException {
		this.config = config;
		// Initialize objects
		mObjectMapper = new ObjectMapper();
		mFieldGenerator = new FieldGenerator(mObjectMapper.readValue(
				new File(getClass().getClassLoader().getResource(FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE).getFile()),
				HashMap.class));
		mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		combinationCount = new HashMap<String, Integer>();

		// Calculate needed environment variables from config
		calculateEnvironment();
	}

	private void calculateEnvironment() {
		averageTimeGapBetween2Records = (double) 3600 * 1000 / (config.getRatePerHour() - 1);
		mExponentialDistribution = new ExponentialDistribution(averageTimeGapBetween2Records);

	}

	public void execute() throws IOException {
		long timeCounter = config.getStartTime();
		while (timeCounter <= config.getStartTime() + config.getDuration() * 1000) {
			String formattedDate = mDateFormat.format(new Date(timeCounter));
			// new Record Template
			RecordTemplate record = new RecordTemplate();
			record.setTimestamp(formattedDate);
			Map<String, String> fieldValues = mFieldGenerator.genFieldValuePairs();
			record.setLevel(fieldValues.getOrDefault("level", DEFAULT_VALUE));
			record.setCat(fieldValues.getOrDefault("cat", DEFAULT_VALUE));
			// LOGGER.info(mObjectMapper.writeValueAsString(record));
			timeCounter += (long) mExponentialDistribution.sample();

			// Record to debug:
			recordCounter++;
			String combination = mObjectMapper.writeValueAsString(fieldValues);
			combinationCount.put(combination, combinationCount.getOrDefault(combination, 0) + 1);
		}
		calculateStatistics();
		LOGGER.info("Record Count: " + recordCounter);
		LOGGER.info("Real Record Rate: " + realRate);
		LOGGER.info("Combination Count: " + combinationCount.toString());
	}

	// Calculate some statistics to make sure the charisteristics of synthetic
	// data
	private void calculateStatistics() {
		realRate = (int) Math.round((double) recordCounter / config.getDuration() * 3600);
	}

}