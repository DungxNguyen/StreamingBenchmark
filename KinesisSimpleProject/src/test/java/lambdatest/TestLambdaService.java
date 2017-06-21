package lambdatest;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

import kinesis.producer.simple.ProducerConfiguration;

public interface TestLambdaService {
	  @LambdaFunction(functionName="MyHelloLambdaFunction")
	  String executeProducer(ProducerConfiguration input);

}
