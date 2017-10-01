package generic.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

//import data.genenator.DataGeneratorParallel;
import data.genenator.FieldGenerator;

public class BenchmarkProducer {

	// CONSTANT
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkProducer.class);
	private static final String DEFAULT_VALUE = "N/A";
	private static final String FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE = "Field_Value_Combination_Distribution.json";
	private static final String METRICS_OUTPUT_FILENAME = "producer.csv";
	private static final int CONSEQUENCE_MAX = 16000001;

	protected BenchmarkProducerConfiguration config;
	// private ExponentialDistribution mExponentialDistribution;
	private DateFormat mDateFormat;
	private FieldGenerator mFieldGenerator;
	private ObjectMapper mObjectMapper;
	private ProducerInterface producer;
	private List<Future<ProducerMetric>> metricsList;// = new ArrayList<>();

	// Environment
	// private double averageTimeGapBetween2Blocks; // in milliseconds

	// Statistics
	private int recordCounter;
	private int realRate;

	public BenchmarkProducer(BenchmarkProducerConfiguration config) throws IOException, URISyntaxException {
		this.config = config;
		// Initialize objects
		mObjectMapper = new ObjectMapper();
		mFieldGenerator = new FieldGenerator(mObjectMapper.readValue(
				getClass().getClassLoader().getResourceAsStream(FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE),
				HashMap.class));
		mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		metricsList = new ArrayList<>();

	}

	public ProducerInterface getProducer() {
		return producer;
	}

	public void setProducer(ProducerInterface producer) {
		this.producer = producer;
	}

	public void execute() throws IOException, InterruptedException, ExecutionException {
		LOGGER.info("Kinesis Stream Execute");

		parallelExecute();

		// producer.flush();
		// Checkcode is the highest id of messages
		// Because mesasge id starts at 0, the highest id is equal to the number
		// Of Message minus 1
		sendCheckingCode(recordCounter - 1);

		// calculateStatistics();
		LOGGER.info("Record Count: " + recordCounter);
		LOGGER.info("Real Record Rate(records per hour): " + realRate);
		LOGGER.info("Real Record Rate(records per second): " + realRate / 3600);
		// LOGGER.info("Combination Count: " + combinationCount.toString());

		producer.flush();

		// Create metrics
		ProducerMetric producerMetrics = new ProducerMetric();
		LOGGER.info("Producer metrics being calucalted:");
		producerMetrics = calculateMetrics();
		producerMetrics.appendToFile(METRICS_OUTPUT_FILENAME);
		LOGGER.info("Reach here");
	}

	private ProducerMetric calculateMetrics() throws InterruptedException, ExecutionException {
		ProducerMetric producerMetrics = new ProducerMetric();
		double bufferingTime = 0;
		double error = 0;
		double retriesPerRecord = 0;
		double dataPerSecond = 0;
		double recordsPerSecond = 0;
		int duration = 0;
		LOGGER.info("Number of producer thread: " + metricsList.size());
		for (Future<ProducerMetric> metricFuture : metricsList) {
			LOGGER.info("Thread is done?: " + metricFuture.isDone());
			ProducerMetric metric = metricFuture.get();
			bufferingTime += metric.bufferingTime;
			error += metric.error;
			retriesPerRecord += metric.retriesPerRecord;
			dataPerSecond += metric.dataPerSecond;
			recordsPerSecond += metric.recordsPerSecond;
			duration += metric.duration;
		}
		// Aggregate data
		producerMetrics.duration = duration / metricsList.size();
		producerMetrics.recordNumber = recordCounter;
		producerMetrics.bufferingTime = bufferingTime / metricsList.size();
		producerMetrics.retriesPerRecord = retriesPerRecord * producerMetrics.duration / producerMetrics.recordNumber;
		producerMetrics.dataPerSecond = dataPerSecond;
		producerMetrics.recordsPerSecond = recordsPerSecond; 
		producerMetrics.recordsPerHour = producerMetrics.recordsPerSecond * 3600;
		producerMetrics.error = (int) (error * producerMetrics.duration);
		
		// Add other info
		producerMetrics.desiredRate = config.getRatePerHour();
		producerMetrics.experimentName = config.getConfigName();
		return producerMetrics;
	}

	private void sendCheckingCode(long checkCode) throws IOException {
		RecordTemplate record = new RecordTemplate();
		record.setMsg(String.valueOf(checkCode));
		record.setCat("CHECKCODE");
		producer.sendMessage(record);
		LOGGER.info("Send checkcode:" + checkCode);
	}

	private void parallelExecute() throws InterruptedException {
		int numberOfThreads = config.getRatePerHour() / CONSEQUENCE_MAX + 1;
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		List<Callable<ProducerMetric>> threads = new ArrayList<Callable<ProducerMetric>>(numberOfThreads);
		for (int i = 0; i < numberOfThreads; i++) {
			threads.add(new ProducerThread(config.getRatePerHour() / numberOfThreads));
		}
		metricsList = executor.invokeAll(threads);
		executor.shutdown();
	}

	public synchronized int registerBlock() {
		recordCounter += config.getBlock();
		return recordCounter - config.getBlock();
	}

	private class ProducerThread implements Callable<ProducerMetric> {
		double threadGap;
		ProducerInterface threadProducer;

		public ProducerThread(int threadRate) {
			if (config.getGap() == -1)
				threadGap = (double) 3600 * 1000 / (threadRate) * config.getBlock();
			else
				threadGap = config.getGap();
			LOGGER.info("Average Thread Gap: " + threadGap);
			threadProducer = producer.createProducer();
		}

		public ProducerMetric execute() throws JsonProcessingException, InterruptedException {
//			ObjectMapper mObjectMapper = new ObjectMapper();
			config.setStartTime(System.currentTimeMillis());

			String randomString = RandomStringUtils.random(1024);

			long windowsStartTime = System.currentTimeMillis();
			while (System.currentTimeMillis() <= config.getStartTime() + config.getDuration() * 1000) {

				int startId = registerBlock();
				for (int i = 0; i < config.getBlock(); i++) {
					RecordTemplate record = new RecordTemplate();
					Map<String, String> fieldValues = mFieldGenerator.genFieldValuePairs();
					record.setLevel(fieldValues.getOrDefault("level", DEFAULT_VALUE));
					record.setCat(fieldValues.getOrDefault("cat", DEFAULT_VALUE));
					record.setTimestamp(mDateFormat.format(new Date(System.currentTimeMillis())));
					record.setMsg(randomString);
					record.setId(startId++);
					record.setTime(System.currentTimeMillis());

					threadProducer.sendMessage(record);
				}

				if (threadGap >= 1) {
					long wait = (long) threadGap + windowsStartTime - System.currentTimeMillis();
					if (wait > 0)
						Thread.sleep(wait);
					windowsStartTime = System.currentTimeMillis();
				}
			}
			threadProducer.flush();
			
			
			// Calculate metric, producer thread level
			ProducerMetric producerMetric = threadProducer.calculateMetric();
			LOGGER.info("Thread duration: " + (System.currentTimeMillis() - config.getStartTime()) / 1000);
			producerMetric.duration = (int) ((System.currentTimeMillis() - config.getStartTime()) / 1000);
			return producerMetric; 
		}

		@Override
		public ProducerMetric call() throws Exception {
			return execute();
		}

	}

}
