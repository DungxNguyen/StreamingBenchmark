package kinesis.producer.simple;

public class ProducerConfiguration {
	private int duration;
	private int rate;
	private int maxRecords;

	public ProducerConfiguration(){
		setDuration(10);
		setRate(100);
		setMaxRecords(0);
	}
	
	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}
	
	public int getRate(){
		return rate;
	}

	public int getMaxRecords() {
		return maxRecords;
	}

	public void setMaxRecords(int maxRecords) {
		this.maxRecords = maxRecords;
	}
}
