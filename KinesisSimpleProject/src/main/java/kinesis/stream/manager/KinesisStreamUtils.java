package kinesis.stream.manager;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class KinesisStreamUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(KinesisStreamUtils.class);

	public static void createStream(String name, int shards, long timeout) {
		AmazonKinesisClient client = (AmazonKinesisClient) AmazonKinesisClientBuilder.standard()
				.withCredentials(new ProfileCredentialsProvider("default"))
				.withEndpointConfiguration(new EndpointConfiguration("kinesis.us-west-1.amazonaws.com", "us-west-1"))
				.build();
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(name);
		createStreamRequest.setShardCount(shards);
		try {
			client.createStream(createStreamRequest);
		} catch (ResourceInUseException e) {
			LOGGER.info(e.getErrorMessage());
			LOGGER.info("Stream " + name + " is already in active");
		}

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(name);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + timeout;
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(20 * 1000);
			} catch (Exception e) {
			}

			try {
				DescribeStreamResult describeStreamResponse = client.describeStream(describeStreamRequest);
				String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
				if (streamStatus.equals("ACTIVE")) {
					LOGGER.info("Stream " + name + " created successfully.");
					// ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
					// listStreamsRequest.setLimit(20);
					// ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
					// List<String> streamNames = listStreamsResult.getStreamNames();
					// for (String streamName : streamNames) {
					// LOGGER.info(streamName);
					// }
					break;
				}
				//
				// sleep for one second
				//
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					LOGGER.info(e.getMessage());
				}
			} catch (ResourceNotFoundException e) {
				LOGGER.info(e.getErrorMessage());
			}
		}
		if (System.currentTimeMillis() >= endTime) {
			throw new RuntimeException("Stream " + name + " never went active");
		}
	}

	public static void deleteStream(String name, long timeout) {
		AmazonKinesisClient client = (AmazonKinesisClient) AmazonKinesisClientBuilder.standard()
				.withCredentials(new ProfileCredentialsProvider("default"))
				.withEndpointConfiguration(new EndpointConfiguration("kinesis.us-west-1.amazonaws.com", "us-west-1"))
				.build();
		try {
			DeleteStreamResult deleteResult = client.deleteStream(name);
			LOGGER.info(deleteResult.getSdkResponseMetadata().toString());
			long startTime = System.currentTimeMillis();
			long endTime = startTime + timeout;
			while (System.currentTimeMillis() < endTime) {
				try {
					Thread.sleep(20 * 1000);
				} catch (Exception e) {
				}

				try {
					DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
					describeStreamRequest.setStreamName(name);
					DescribeStreamResult describeStreamResponse = client.describeStream(describeStreamRequest);
					String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
					if (streamStatus.equals("DELETING")) {
						LOGGER.info("Stream " + name + " is being deleted.");
					}
					//
					// sleep for one second
					//
					try {
						Thread.sleep(1000);
					} catch (Exception e) {
						LOGGER.info(e.getMessage());
						break;
					}
				} catch (ResourceNotFoundException e) {
					LOGGER.info(e.getErrorMessage());
					LOGGER.info("Stream " + name + " is deleted.");
					break;
				}
			}
			if (System.currentTimeMillis() >= endTime) {
				throw new RuntimeException("Stream " + name + " never been deleted");
			}
			// ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
			// listStreamsRequest.setLimit(20);
			// ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
			// List<String> streamNames = listStreamsResult.getStreamNames();
			// for (String streamName : streamNames) {
			// LOGGER.info(streamName);
			// }
		} catch (Exception e) {
			LOGGER.info(e.getMessage());
		}
	}

	public static void printListStream() {
		AmazonKinesisClient client = (AmazonKinesisClient) AmazonKinesisClientBuilder.standard()
				.withCredentials(new ProfileCredentialsProvider("default"))
				.withEndpointConfiguration(new EndpointConfiguration("kinesis.us-west-1.amazonaws.com", "us-west-1"))
				.build();
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(20);
		ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		for (String streamName : streamNames) {
			LOGGER.info(streamName);
		}
	}

	public static void main(String[] args) {
		// createStream("testNewStream", 2, 300000);
		deleteStream("testNewStream", 300000);
		createStream("testNewStream", 2, 300000);
		printListStream();
	}
}
