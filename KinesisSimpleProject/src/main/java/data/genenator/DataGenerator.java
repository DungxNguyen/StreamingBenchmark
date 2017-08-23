package data.genenator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.Metric;
import com.fasterxml.jackson.databind.ObjectMapper;

import kinesis.common.RecordTemplate;

public class DataGenerator {
	// CONSTANT
	private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);
	private static final String DEFAULT_VALUE = "N/A";
	private static final String FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE = "Field_Value_Combination_Distribution.json";
	private static final String KINESIS_STREAM_NAME = KinesisTestSuite.STREAM_NAME;
	private static final String METRICS_OUTPUT_FILENAME = "producer.csv";
	// private static final int BLOCK_SIZE = 3;

	private DataGeneratorConfiguration config;
	private ExponentialDistribution mExponentialDistribution;
	private DateFormat mDateFormat;
	private FieldGenerator mFieldGenerator;
	private ObjectMapper mObjectMapper;
	private Random mRandom;
	private long checkCode = 0;

	// Environment
	private double averageTimeGapBetween2Blocks; // in milliseconds

	// Statistics
	private int recordCounter;
	private int realRate;
	private Map<String, Integer> combinationCount;

	public DataGenerator(DataGeneratorConfiguration config) throws IOException, URISyntaxException {
		this.config = config;
		// Initialize objects
		mObjectMapper = new ObjectMapper();
		mFieldGenerator = new FieldGenerator(mObjectMapper.readValue(
				getClass().getClassLoader().getResourceAsStream(FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE),
				HashMap.class));
		mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		combinationCount = new HashMap<String, Integer>();

		// Calculate needed environment variables from config
		calculateEnvironment();
	}

	private void calculateEnvironment() {
		if (config.getGap() == -1)
			averageTimeGapBetween2Blocks = (double) 3600 * 1000 / (config.getRatePerHour() - 1) * config.getBlock();
		else
			averageTimeGapBetween2Blocks = config.getGap();
		LOGGER.info("Average Gap: " + averageTimeGapBetween2Blocks);
		// mExponentialDistribution = new
		// ExponentialDistribution(averageTimeGapBetween2Blocks);
		mRandom = new Random();
	}

	// public void executeLocal() throws IOException {
	// LOGGER.info("Local Execute");
	// long timeCounter = config.getStartTime();
	// while (timeCounter <= config.getStartTime() + config.getDuration() * 1000) {
	// String formattedDate = mDateFormat.format(new Date(timeCounter));
	// // new Record Template
	// RecordTemplate record = new RecordTemplate();
	// record.setTimestamp(formattedDate);
	// Map<String, String> fieldValues = mFieldGenerator.genFieldValuePairs();
	// record.setLevel(fieldValues.getOrDefault("level", DEFAULT_VALUE));
	// record.setCat(fieldValues.getOrDefault("cat", DEFAULT_VALUE));
	// // LOGGER.info(mObjectMapper.writeValueAsString(record));
	// timeCounter += (long) mExponentialDistribution.sample();
	//
	// // Record to debug:
	// recordCounter++;
	// String combination = mObjectMapper.writeValueAsString(fieldValues);
	// combinationCount.put(combination, combinationCount.getOrDefault(combination,
	// 0) + 1);
	// }
	// calculateStatistics();
	// LOGGER.info("Record Count: " + recordCounter);
	// LOGGER.info("Real Record Rate: " + realRate);
	// LOGGER.info("Combination Count: " + combinationCount.toString());
	// }

	public void executeKinesis() throws IOException, InterruptedException, ExecutionException {
		LOGGER.info("Kinesis Stream Execute");

		KinesisProducer kinesis = new KinesisProducer(
				new KinesisProducerConfiguration().setRegion("us-west-1").setAggregationEnabled(true).
				// setRecordTtl(9223372036854775807L));
						setRecordTtl(3600000000L));

		config.setStartTime(System.currentTimeMillis());

		String randomString = RandomStringUtils.random(1024);

		int startId = 0;

		long windowsStartTime = System.currentTimeMillis();
		while (System.currentTimeMillis() <= config.getStartTime() + config.getDuration() * 1000) {
			windowsStartTime = System.currentTimeMillis();

			for (int i = 0; i < config.getBlock(); i++) {
				// new Record Template
				RecordTemplate record = new RecordTemplate();
				Map<String, String> fieldValues = mFieldGenerator.genFieldValuePairs();
				record.setLevel(fieldValues.getOrDefault("level", DEFAULT_VALUE));
				record.setCat(fieldValues.getOrDefault("cat", DEFAULT_VALUE));
				record.setTimestamp(mDateFormat.format(new Date(System.currentTimeMillis())));
				record.setMsg(randomString);
				record.setId(startId++);
				record.setTime(System.currentTimeMillis());

				// Send record to Kinesis Stream
				kinesis.addUserRecord(KINESIS_STREAM_NAME, String.format("partitionKey-%d", windowsStartTime),
						ByteBuffer.wrap(mObjectMapper.writeValueAsBytes(record)));

				// Add the code
				checkCode = record.getId();

				// Record to examine the settings:
				recordCounter++;

				// Set up sleep time, expect each iteration has total time based on exponential
				// dist
				// with mean = average delay
				// Thread.sleep((long) mExponentialDistribution.sample() - windowsStartTime +
				// System.currentTimeMillis());
			}

			if (averageTimeGapBetween2Blocks >= 1) {
				long wait = (long) averageTimeGapBetween2Blocks + windowsStartTime - System.currentTimeMillis();
				if (wait > 0)
					Thread.sleep(wait);
				windowsStartTime = System.currentTimeMillis();
			}
			// Count combination for data quality
			// String combination =
			// mObjectMapper.writeValueAsString(fieldValues);
			// combinationCount.put(combination,
			// combinationCount.getOrDefault(combination, 0) + 1);
		}

		waitKinesis(kinesis);
		sendCheckingCode(kinesis, checkCode);

		calculateStatistics();
		LOGGER.info("Record Count: " + recordCounter);
		LOGGER.info("Real Record Rate(records per hour): " + realRate);
		LOGGER.info("Real Record Rate(records per second): " + realRate / 3600);
		// LOGGER.info("Combination Count: " + combinationCount.toString());

		waitKinesis(kinesis);

		// Create metrics
		KinesisMetric kinesisMetrics = new KinesisMetric();
		LOGGER.info("Producer metrics:");
		List<Metric> metrics = kinesis.getMetrics();
		Collections.sort(metrics, (x, y) -> x.getName().compareToIgnoreCase(y.getName()));
		for (Metric metric : metrics) {
			if (!metric.getDimensions().containsKey("ShardId") && metric.getDimensions().containsKey("StreamName")) {
				LOGGER.info(metric.toString());
				if (metric.getName().equalsIgnoreCase("bufferingtime")) {
					kinesisMetrics.bufferingTime = metric.getMean();
				} else if (metric.getName().equalsIgnoreCase("allerrors")) {
					kinesisMetrics.error = (int) metric.getSum();
				} else if (metric.getName().equalsIgnoreCase("retriesperrecord")) {
					kinesisMetrics.retriesPerRecord = metric.getMean();
				} else if (metric.getName().equalsIgnoreCase("userrecordsdataput")) {
					kinesisMetrics.dataPerSecond = metric.getSum() / metric.getDuration() / 1024;
				} else if (metric.getName().equalsIgnoreCase("userrecordsput")) {
					kinesisMetrics.recordsPerSecond = metric.getSum() / metric.getDuration();
					kinesisMetrics.recordsPerHour = kinesisMetrics.recordsPerSecond * 3600;
				}
			}
		}

		kinesisMetrics.desiredRate = config.getRatePerHour();
		kinesisMetrics.recordNumber = recordCounter;
		kinesisMetrics.experimentName = config.getConfigName();
		kinesisMetrics.appendToFile(METRICS_OUTPUT_FILENAME);
	}

	private void sendCheckingCode(KinesisProducer kinesis, long checkCode) throws IOException {
		RecordTemplate record = new RecordTemplate();
		record.setMsg(String.valueOf(checkCode));
		record.setCat("CHECKCODE");
		kinesis.addUserRecord(KINESIS_STREAM_NAME, String.format("partitionKey-%d", System.currentTimeMillis()),
				ByteBuffer.wrap(mObjectMapper.writeValueAsBytes(record)));
		LOGGER.info("Send checkcode:" + checkCode);
	}

	private void waitKinesis(KinesisProducer kinesis) {
		kinesis.flushSync();
	}

	// Calculate some statistics to make sure the charisteristics of synthetic
	// data
	private void calculateStatistics() {
		realRate = (int) Math.round((double) recordCounter / config.getDuration() * 3600);
	}

}