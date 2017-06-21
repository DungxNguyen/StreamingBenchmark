package kinesis.consumer.simple;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

public class SimpleConsumerRecordProcessorFactory implements IRecordProcessorFactory {

	@Override
	public IRecordProcessor createProcessor() {
		return new SimpleConsumerRecordProcessor();
	}

}
