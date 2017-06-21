package kinesis.producer.simple;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kinesis.common.RecordTemplate;

public class SyntheticProducerKPL {

	private static final Logger logger = LoggerFactory.getLogger(SyntheticProducerKPL.class);
	private int rate; // The rate of records per second
	private int runningTime; // The running time in seconds
	private int maxRecords; // The maximum number of records
	private KinesisProducerConfiguration config;
	private KinesisProducer kinesis;

	public static void main(String[] args)
			throws UnsupportedEncodingException, InterruptedException, JsonProcessingException {
		ProducerConfiguration config = new ProducerConfiguration();
		config.setRate(5);
		config.setDuration(300);
		config.setMaxRecords(0);
		SyntheticProducerKPL testProducer = new SyntheticProducerKPL(config);
		testProducer.execute();
		Thread.sleep(1000);
	}

	public SyntheticProducerKPL() {
		config = new KinesisProducerConfiguration();
		config.setRegion("us-west-1");
		config.setAggregationEnabled(true);
		kinesis = new KinesisProducer(config);

	}

	public SyntheticProducerKPL(ProducerConfiguration producerConfig) {
		this();
		setRunningTime(producerConfig.getDuration());
		setRate(producerConfig.getRate());
		setMaxRecords(producerConfig.getMaxRecords());
	}

	public void execute() throws UnsupportedEncodingException, InterruptedException, JsonProcessingException {
		long startTime = System.nanoTime();
		long currentTime = System.nanoTime();
		int recordCount = 0;
		ObjectMapper jsonObjectMapper = new ObjectMapper();

		while (currentTime - startTime <= getRunningTime() * 1000000000L) {
			currentTime = System.nanoTime();
			RecordTemplate record = RecordTemplate.genRandomData(10);
			kinesis.addUserRecord("CoffeeStream", String.format("partitionKey-%d", currentTime),
					ByteBuffer.wrap(jsonObjectMapper.writeValueAsBytes(record)));
			// ByteBuffer.wrap(jsonObjectMapper.writeValueAsString(record)));
			recordCount++;
			if (maxRecords > 0 && recordCount >= maxRecords) {
				break;
			}
			nanoWait(1000000000L / getRate() - System.nanoTime() + currentTime);
		}
		long totalTime = (System.nanoTime() - startTime) / 1000000000L;
		logger.info("Record Count: " + recordCount);
		logger.info("Running Time: " + totalTime + " seconds");
		logger.info("Record Rate per sec: " + (double) recordCount / totalTime);
	}

	private void nanoWait(long nanoDelay) {
		long start = System.nanoTime();
		while (start + nanoDelay >= System.nanoTime()) {
		}
	}

	// Getter and Setter
	public int getRate() {
		return rate;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}

	public int getRunningTime() {
		return runningTime;
	}

	public void setRunningTime(int runningTime) {
		this.runningTime = runningTime;
	}

	public int getMaxRecords() {
		return maxRecords;
	}

	public void setMaxRecords(int maxRecords) {
		this.maxRecords = maxRecords;
	}
}
