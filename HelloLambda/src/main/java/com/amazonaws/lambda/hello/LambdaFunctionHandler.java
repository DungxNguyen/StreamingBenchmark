package com.amazonaws.lambda.hello;

import java.io.UnsupportedEncodingException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<ProducerConfiguration, String> {

	// @Override
	// public String handleRequest(Object input, Context context) {
	// context.getLogger().log("Input: " + input + "\n");
	//
	// // TODO: implement your handler
	// long currentTime = System.currentTimeMillis();
	// context.getLogger().log("Time: " + currentTime + "\n" );
	// context.getLogger().log("Function Name" + context.getFunctionName());
	//
	// try {
	// Thread.sleep(10000L);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	//
	// return String.valueOf(currentTime);
	// }

	@Override
	public String handleRequest(ProducerConfiguration input, Context context) {
		context.getLogger().log("Input: " + input + "\n");
		SyntheticProducerKPL producer = new SyntheticProducerKPL(input);
		try {
			producer.execute();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
