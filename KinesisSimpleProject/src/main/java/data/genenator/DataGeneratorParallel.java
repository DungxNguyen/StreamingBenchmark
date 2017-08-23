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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kinesis.common.RecordTemplate;

public class DataGeneratorParallel {
	// CONSTANT
	private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorParallel.class);
	private static final String DEFAULT_VALUE = "N/A";
	private static final String FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE = "Field_Value_Combination_Distribution.json";
	private static final String KINESIS_STREAM_NAME = KinesisTestSuite.STREAM_NAME;
	private static final String METRICS_OUTPUT_FILENAME = "producer.csv";
	private static final int CONSEQUENCE_MAX = 16000001;
	// private static final int BLOCK_SIZE = 3;

	private DataGeneratorConfiguration config;
//	private ExponentialDistribution mExponentialDistribution;
	private DateFormat mDateFormat;
	private FieldGenerator mFieldGenerator;
	private ObjectMapper mObjectMapper;
	private KinesisProducer kinesis;

	// Environment
//	private double averageTimeGapBetween2Blocks; // in milliseconds

	// Statistics
	private int recordCounter;
	private int realRate;

	public DataGeneratorParallel(DataGeneratorConfiguration config) throws IOException, URISyntaxException {
		this.config = config;
		// Initialize objects
		mObjectMapper = new ObjectMapper();
		mFieldGenerator = new FieldGenerator(mObjectMapper.readValue(
				getClass().getClassLoader().getResourceAsStream(FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE),
				HashMap.class));
		mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		combinationCount = new HashMap<String, Integer>();
		kinesis = new KinesisProducer(
				new KinesisProducerConfiguration().setRegion("us-west-1").setAggregationEnabled(true).
				// setRecordTtl(9223372036854775807L));
						setRecordTtl(3600000000L));

		// Calculate needed environment variables from config
//		calculateEnvironment();
	}

//	private void calculateEnvironment() {
//		LOGGER.info("Average Gap: " + averageTimeGapBetween2Blocks);
//		// mExponentialDistribution = new
//		// ExponentialDistribution(averageTimeGapBetween2Blocks);
//	}

	public void executeKinesis() throws IOException, InterruptedException, ExecutionException {
		LOGGER.info("Kinesis Stream Execute");

		parallelExecute();
		
		waitKinesis(kinesis);
		sendCheckingCode(kinesis, recordCounter - 1);

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

	public synchronized int registerBlock() {
		recordCounter += config.getBlock();
		return recordCounter;
	}

	public KinesisProducer getKinesis() {
		return this.kinesis;
	}
	
	public void parallelExecute() {
		int numberOfThreads = config.getRatePerHour() / CONSEQUENCE_MAX + 1;
		Thread[] executionPool = new Thread[numberOfThreads];
		for (int i = 0; i < numberOfThreads; i++) {
			executionPool[i] = new Thread(new ProducerThread(config.getRatePerHour() / numberOfThreads));
			executionPool[i].start();
		}
		for (Thread thread : executionPool) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private class ProducerThread implements Runnable {

		double threadGap;

		public ProducerThread(int threadRate) {
			if (config.getGap() == -1)
				threadGap = (double) 3600 * 1000 / (threadRate) * config.getBlock();
			else
				threadGap = config.getGap();
			LOGGER.info("Average Thread Gap: " + threadGap);
		}

		@Override
		public void run() {
			try {
				execute();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void execute() throws JsonProcessingException, InterruptedException {
			ObjectMapper mObjectMapper = new ObjectMapper();
			config.setStartTime(System.currentTimeMillis());

			String randomString = RandomStringUtils.random(1024);

			long windowsStartTime = System.currentTimeMillis();
			while (System.currentTimeMillis() <= config.getStartTime() + config.getDuration() * 1000) {

				int startId = registerBlock();
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
					getKinesis().addUserRecord(KINESIS_STREAM_NAME, String.format("partitionKey-%d", windowsStartTime),
							ByteBuffer.wrap(mObjectMapper.writeValueAsBytes(record)));

					// Set up sleep time, expect each iteration has total time based on exponential
					// dist
					// with mean = average delay
					// Thread.sleep((long) mExponentialDistribution.sample() - windowsStartTime +
					// System.currentTimeMillis());
				}

				if (threadGap >= 1) {
					long wait = (long) threadGap + windowsStartTime - System.currentTimeMillis();
					if (wait > 0)
						Thread.sleep(wait);
					windowsStartTime = System.currentTimeMillis();
				}
			}
		}

	}

}