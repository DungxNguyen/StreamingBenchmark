package kinesis.consumer.benchmark;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.fasterxml.jackson.databind.ObjectMapper;

import generic.producer.RecordTemplate;

public class BenchmarkConsumerRecordProcessor implements IRecordProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkConsumerRecordProcessor.class);

	private BenchmarkConsumerWorker worker;

	boolean start = false;
	ObjectMapper objectMapper = new ObjectMapper();

	public BenchmarkConsumerRecordProcessor(BenchmarkConsumerWorker worker) {
		this.worker = worker;
	}

	@Override
	public void initialize(InitializationInput arg0) {
		LOGGER.info("Initialize: " + arg0);
	}

	@Override
	public void processRecords(ProcessRecordsInput arg0) {
		for (com.amazonaws.services.kinesis.model.Record rawRecord : arg0.getRecords()) {
			try {
				byte[] data = rawRecord.getData().array();
				RecordTemplate record = objectMapper.readValue(data, RecordTemplate.class);
				BenchmarkConsumerWorker.addCapacity(data.length);
				if (!start) {
					BenchmarkConsumerWorker.setStartingTime();
					start = true;
				}
				if (record.getCat().equals("CHECKCODE")) {
					LOGGER.info("Receive CheckCode: " + record.getMsg());
					worker.processCheckCode(Long.valueOf(record.getMsg()));
				} else {
					worker.addId(record.getId());
					BenchmarkConsumerWorker.addLatency(System.currentTimeMillis() - record.getTime());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void shutdown(ShutdownInput arg0) {
		// TODO Auto-generated method stub
	}

}
