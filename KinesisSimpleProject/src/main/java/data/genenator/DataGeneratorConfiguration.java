package data.genenator;

public class DataGeneratorConfiguration {
	private int ratePerHour; // number of message per hour
	private int duration; // duration in seconds
	private long startTime; // epoch start time
	private String configName;

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
}
