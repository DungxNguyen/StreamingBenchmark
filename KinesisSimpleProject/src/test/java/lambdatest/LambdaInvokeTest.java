package lambdatest;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;

import kinesis.producer.simple.ProducerConfiguration;

public class LambdaInvokeTest {
	private static final int NUMBER_OF_THREADS = 3;
	private static final int DURATION_OF_RUNNING = 30;
	private static final int RATE = 10000;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// final TestLambdaService testLambdaService = LambdaInvokerFactory.builder()
		// .lambdaClient(AWSLambdaClientBuilder.defaultClient())
		// .build(TestLambdaService.class);
		//
		// System.out.println(testLambdaService.hello("AWS Lambda World"));
		LambdaInvokeTest test = new LambdaInvokeTest();
		test.runTest();
	}

	public void runTest() {
		ProducerConfiguration producerConfiguration = new ProducerConfiguration();
		producerConfiguration.setDuration(DURATION_OF_RUNNING);
		producerConfiguration.setRate(RATE);
		LambdaInvokeTestThread[] threads = new LambdaInvokeTestThread[NUMBER_OF_THREADS];
		for (int i = 0; i < NUMBER_OF_THREADS; i++) {
			threads[i] = new LambdaInvokeTestThread();
			threads[i].setProducerConfiguration(producerConfiguration);
			Thread t = new Thread(threads[i]);
			t.start();
		}
	}

	class LambdaInvokeTestThread implements Runnable {
		private ProducerConfiguration producerConfiguration;

		@Override
		public void run() {
			final TestLambdaService testLambdaService = LambdaInvokerFactory.builder()
					.lambdaClient(AWSLambdaClientBuilder.defaultClient()).build(TestLambdaService.class);

			testLambdaService.executeProducer(getProducerConfiguration());
		}

		public ProducerConfiguration getProducerConfiguration() {
			return producerConfiguration;
		}

		public void setProducerConfiguration(ProducerConfiguration producerConfiguration) {
			this.producerConfiguration = producerConfiguration;
		}
	}
}
