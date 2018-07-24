package com.davromalc.kafka.streams.bootstrap;

import static com.davromalc.kafka.streams.model.Accounts.TOPIC;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER1;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER2;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER3;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER4;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER5;
import static com.davromalc.kafka.streams.model.Accounts.accounts;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.Consumer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import lombok.extern.slf4j.Slf4j;


@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = KafkaStreamsConfiguration.class)
@EmbeddedKafka(topics = { TOPIC,
		TOPIC_CUSTOMER1,
		TOPIC_CUSTOMER2,
		TOPIC_CUSTOMER3,
		TOPIC_CUSTOMER4,
		TOPIC_CUSTOMER5})
@Slf4j
public class DispatcherKStreamBuilderTest {
	

	
	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	Consumer<String, String> consumerTopic;
	
	@Autowired
	Consumer<String, String> consumerCustomerTopic1;
	
	@Autowired
	Consumer<String, String> consumerCustomerTopic2;
	
	@Autowired
	Consumer<String, String> consumerCustomerTopic3;
	
	@Autowired
	Consumer<String, String> consumerCustomerTopic4;
	
	@Autowired
	Consumer<String, String> consumerCustomerTopic5;
	
	private final Executor executor = Executors.newCachedThreadPool();

	@Test
	@DisplayName("Given several accounts When the stream process the messages Then the messages should be redirected to customer topics")
	public void givenSeveralAccountsWhenTheStreamProcessTheMessagesThenTheMessagesShouldBeRedirectedToCustomerTopics() throws InterruptedException {
		executor.execute(() -> {
			for ( String account : accounts ) {
				template.send(TOPIC, account );
			}
		});

		Awaitility.await().atMost(Duration.FIVE_MINUTES).pollInterval(Duration.FIVE_SECONDS).until(() -> {
			int messages = consumerTopic.poll(1000).count();
			log.info("We have {} messages", messages);
			return messages == 0;
		});
		
		assertAll(
				() -> assertEquals(3 ,  consumerCustomerTopic1.poll(1000).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic2.poll(1000).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic3.poll(1000).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic4.poll(1000).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic5.poll(1000).count()));
	}

}
