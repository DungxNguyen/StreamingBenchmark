package generic.producer;

import java.util.concurrent.Future;
import kinesis.common.*;

public interface ProducerInterface<T> {
	//TODO Static to ccreate new instance of Producer
	public ProducerInterface<T> createProducer();
	public Future<T> sendMessage(RecordTemplate record);
	public void flush();
}
