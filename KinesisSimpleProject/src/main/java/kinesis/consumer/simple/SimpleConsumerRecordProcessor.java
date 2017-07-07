package kinesis.consumer.simple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.fasterxml.jackson.databind.ObjectMapper;

import kinesis.common.RecordTemplate;

public class SimpleConsumerRecordProcessor implements IRecordProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerRecordProcessor.class);

	@Override
	public void initialize(InitializationInput arg0) {
		LOGGER.info("Initialize: " + arg0);
	}

	@Override
	public void processRecords(ProcessRecordsInput arg0) {
		LOGGER.info("Process: " + arg0.getRecords().size());
		Map<String, Integer> logCount = new HashMap<String, Integer>();
		ObjectMapper objectMapper = new ObjectMapper();
		for( com.amazonaws.services.kinesis.model.Record rawRecord : arg0.getRecords() ){
			LOGGER.info("Record: " + new String(rawRecord.getData().array()));
			try {
				RecordTemplate record = objectMapper.readValue(rawRecord.getData().array(), RecordTemplate.class);
				if (logCount.containsKey(record.getLevel())){
					logCount.put(record.getLevel(), logCount.get(record.getLevel()) + 1);
				} else {
					logCount.put(record.getLevel(), 1);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for(Map.Entry<String, Integer> entry : logCount.entrySet()){
			SimpleConsumerWorker.count(entry.getKey(), entry.getValue());
		}
		SimpleConsumerWorker.printLogCount();
	}

	@Override
	public void shutdown(ShutdownInput arg0) {
		// TODO Auto-generated method stub

	}

}
