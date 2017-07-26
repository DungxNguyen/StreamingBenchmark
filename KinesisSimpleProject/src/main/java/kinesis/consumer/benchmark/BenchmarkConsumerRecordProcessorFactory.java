package kinesis.consumer.benchmark;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

public class BenchmarkConsumerRecordProcessorFactory implements IRecordProcessorFactory {

	private BenchmarkConsumerWorker worker;

	public BenchmarkConsumerRecordProcessorFactory(BenchmarkConsumerWorker worker) {
		this.worker = worker;
	}

	@Override
	public IRecordProcessor createProcessor() {
		return new BenchmarkConsumerRecordProcessor(worker);
	}

}
