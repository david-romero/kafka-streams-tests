package com.davromalc.kafka.streams.core.kafka.dispatcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.davromalc.kafka.streams.core.logging.ReflectionException;


public class DispatcherKStreamBuilderWithOutSpringTest {
	
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
	
	
	TopologyTestDriver testDriver;
	
	ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(TOPIC, new StringSerializer(), new StringSerializer());
	
	@BeforeEach
	public void setUp() throws ReflectionException {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		StreamsBuilder streamBuilder = new StreamsBuilder();
		DispatcherKStreamBuilder builder = new DispatcherKStreamBuilder(streamBuilder, TOPIC, SYMBOL_TOPIC_MAP);
		testDriver = new TopologyTestDriver(builder.build(), config);
	}


	@Test
	@DisplayName("Given An Only Customer and Many Messages When Stream Is Invoked Then a Single Email Must Be Sent")
	public void givenAOnlyCustomerAndManyMessagesWhenStreamIsInvokedThenASingleEmailMustBeSent() throws InterruptedException {
		for ( String account : accounts ) {
			sendMessage(account);
		}

		
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER1), null, accounts[0]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER2), null, accounts[1]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER3), null, accounts[2]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER3), null, accounts[3]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER4), null, accounts[4]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER5), null, accounts[5]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER1), null, accounts[6]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER2), null, accounts[7]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER1), null, accounts[8]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER4), null, accounts[9]);
		OutputVerifier.compareKeyValue(readMessage(TOPIC_CUSTOMER5), null, accounts[10]);
		
	}
	
	
	@AfterEach
	public void tearDown() {
		testDriver.close();
	}
	
	private void sendMessage(String message) {
		
		String key = null;
		KeyValue<String,String> kv = new KeyValue<String, String>(key, message);
		List<KeyValue<String,String>> keyValues = java.util.Arrays.asList(kv);
		List<ConsumerRecord<byte[], byte[]>> create = factory.create(keyValues);
		testDriver.pipeInput(create);
	}
	
	private ProducerRecord<String, String> readMessage(String topic) {
		return testDriver.readOutput(topic, new StringDeserializer(), new StringDeserializer());
	}

}
