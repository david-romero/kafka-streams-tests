package com.davromalc.kafka.streams.core.kafka.dispatcher;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.davromalc.kafka.streams.bootstrap.dispatcher.KafkaStreamsConfiguration;

import lombok.extern.slf4j.Slf4j;


@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = KafkaStreamsConfiguration.class)
@EmbeddedKafka(topics = { DispatcherKStreamBuilderTest.TOPIC,
		DispatcherKStreamBuilderTest.TOPIC_CUSTOMER1,
		DispatcherKStreamBuilderTest.TOPIC_CUSTOMER2,
		DispatcherKStreamBuilderTest.TOPIC_CUSTOMER3,
		DispatcherKStreamBuilderTest.TOPIC_CUSTOMER4,
		DispatcherKStreamBuilderTest.TOPIC_CUSTOMER5})
@Slf4j
public class DispatcherKStreamBuilderTest {
	
	public static final String TOPIC = "account-input";
	public static final String TOPIC_CUSTOMER1 = "customer-1";
	public static final String TOPIC_CUSTOMER2 = "customer-2";
	public static final String TOPIC_CUSTOMER3 = "customer-3";
	public static final String TOPIC_CUSTOMER4 = "customer-4";
	public static final String TOPIC_CUSTOMER5 = "customer-5";
	
	private final String[] accounts = new String[] {
			RandomStringUtils.randomAlphabetic(15) + "12345" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "55555" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "54321" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "54321" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "11111" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "33445" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "12345" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "55555" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "12345" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "11111" + RandomStringUtils.randomAlphabetic(30),
			RandomStringUtils.randomAlphabetic(15) + "33445" + RandomStringUtils.randomAlphabetic(30)
	};
	
	public static final Map<String,String> SYMBOL_TOPIC_MAP = new HashMap<>(5);
	
	static {
		SYMBOL_TOPIC_MAP.put("12345", TOPIC_CUSTOMER1);
		SYMBOL_TOPIC_MAP.put("55555", TOPIC_CUSTOMER2);
		SYMBOL_TOPIC_MAP.put("54321", TOPIC_CUSTOMER3);
		SYMBOL_TOPIC_MAP.put("11111", TOPIC_CUSTOMER4);
		SYMBOL_TOPIC_MAP.put("33445", TOPIC_CUSTOMER5);
	}
	
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
	
	@Autowired
	KafkaEmbedded embeddedKafka;
	
	private final Executor executor = Executors.newCachedThreadPool();

	@Test
	@DisplayName("Given An Only Customer and Many Messages When Stream Is Invoked Then a Single Email Must Be Sent")
	public void givenAOnlyCustomerAndManyMessagesWhenStreamIsInvokedThenASingleEmailMustBeSent() throws InterruptedException {
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
		
		Thread.sleep(40*1000);
		
		
		assertAll(
				() -> assertEquals(3 ,  consumerCustomerTopic1.poll(1).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic2.poll(1).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic3.poll(1).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic4.poll(1).count()),
				() -> assertEquals(2 ,  consumerCustomerTopic5.poll(1).count()));
	}

}
