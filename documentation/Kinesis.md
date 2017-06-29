# Kinesis Documentation
* Amazon Kinesis is a streaming platform, which works as a middle layer between data producer (cars, webserver, etc) and data consumer (database, analyzer, etc).
* Alternative to Kafka
* Can store data up to 1 weeks (AWS docs)
* High availability, managed by Amazon.
* Data stored in sequence
* A shard is Kinesis' unit.
* Each shard can handle 1 MB/s input and 2 MB/s output (AWS docs)
* No limit on number of shard per Stream
* When change the # of shards, old shards are deleted and AWS put those records to new shards

## Common
* 2 ways to use Kinesis
** AWS API: Official, well-supported, updated, multi-platform (Java/Python/etc)
** Kinesis Libraries: Java(could be binded into other languages), not-so-well documented, feature-rich

## Kinesis Producer (library)
* To achieve good performance, we need to batching methods. API doesn't provide batching (AWS docs)
* Full features only available on Java application
* Aggregation/Production: combine multiple message into 1 records, multiple records into 1 request
* Guarantee mechanism: Error handle/retry (Need to implement if using API)
* Async delivery: No blocking when deliver messages (Need to implement if using API, though it's simple)

## Kinesis Consumer (library)
* Maximum 10MB each request
* Each Stream has a set of Applications
* Worker/Processor mechanism to load-balance the Stream (Need to implement if using API, AWS Docs)
* 1 Processor each shard. Multiple processors each worker. Workers in the same Application can commuicate through Amazon DynamoDB to load-balance
* The number of active processors is equal to the number of shards
* Can deploy workers for fail-over purpose
* Automatically adapt the change in Stream (increase/decrease number of shards)

## Next
* Can be integrate to Apache Spark
