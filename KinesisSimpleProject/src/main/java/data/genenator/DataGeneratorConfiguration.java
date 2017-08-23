package data.genenator;

public class DataGeneratorConfiguration {
	private int ratePerHour; // number of message per hour
	private int duration; // duration in seconds
	private long startTime; // epoch start time
	private String configName;
	private int gap;
	private int block;

	public int getRatePerHour() {
		return ratePerHour;
	}

	public void setRatePerHour(int ratePerHour) {
		this.ratePerHour = ratePerHour;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public int getGap() {
		return gap;
	}

	public void setGap(int gap) {
		this.gap = gap;
	}

	public int getBlock() {
		return block;
	}

	public void setBlock(int block) {
		this.block = block;
	}
}
