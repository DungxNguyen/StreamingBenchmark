package com.amazonaws.lambda.hello;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class SyntheticProducerKPL {

	private static final Logger logger = LoggerFactory.getLogger(SyntheticProducerKPL.class);
	private int rate; // The rate of records per second
	private int runningTime; // The running time in seconds
	private KinesisProducerConfiguration config;
	private KinesisProducer kinesis;

	public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {
		SyntheticProducerKPL testProducer = new SyntheticProducerKPL();
		testProducer.setRate(4000);
		testProducer.setRunningTime(300);
		testProducer.execute();
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
	}

	public void execute() throws UnsupportedEncodingException, InterruptedException {
		long startTime = System.nanoTime();
		long currentTime = System.nanoTime();
		int recordCount = 0;
		while (currentTime - startTime <= getRunningTime() * 1000000000L) {
			currentTime = System.nanoTime();
			String record = RandomStringUtils.randomAlphanumeric(1024);
			kinesis.addUserRecord("CoffeeStream", String.format("partitionKey-%d", currentTime),
					ByteBuffer.wrap(record.getBytes("UTF-8")));
			recordCount++;
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
}
