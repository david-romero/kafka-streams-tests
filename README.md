# kafka-streams-tests

Unit test examples for Kafka Streams

Repository for the blog entry [How to test Kafka-Streams](https://david-romero.github.io//articles/2018-07/kafka-streams-test)

In this repository we have three different kafka-stream examples:

1. Bank Balance: Extracted from [udemy](https://www.udemy.com/kafka-streams/). Process all incoming transactions and accumulate them in a balance.
2. Customer purchases dispatcher: Process all incoming purchases and dispatch to the specific customer topic informed in the purchase code received.
3. Menu preparation: For each customer, the stream receives several recipes and this recipes must be grouped into a menu and sent by email to the customer. A single email should be received by each customer

For the above scenarios, we have unit and/or integration tests.
1. Unit tests has been developed with [kafka-streams-test-utils](https://kafka.apache.org/documentation/streams/developer-guide/testing.html) library.
2. Integration tests has been developed with [spring-kafka-test](https://github.com/spring-projects/spring-kafka/blob/master/src/reference/asciidoc/testing.adoc) library.

## How to run
1. clone the repository
2. mvn clean test
