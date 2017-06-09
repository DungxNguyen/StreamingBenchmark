package com.amazonaws.lambda.hello;

public class ProducerConfiguration {
	private int duration;

	public ProducerConfiguration(){
		setDuration(10);
	}
	
	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}
	
	
}
