package com.davromalc.kafka.streams.bootstrap;

import static com.davromalc.kafka.streams.model.Accounts.SYMBOL_TOPIC_MAP;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER1;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER2;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER3;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER4;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER5;
import static java.util.Arrays.stream;

import java.util.List;
import java.util.Properties;

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

import com.davromalc.kafka.streams.logging.ReflectionException;
import com.davromalc.kafka.streams.model.Accounts;


public class DispatcherKStreamBuilderWithOutSpringTest {
		
	
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
	@DisplayName("Given Ten Codes And A Map With Customer Codes And Target Topics When The Stream Receives Ten Codes Then Every Target Topic Should Receive Its PurchaseCode")
	public void givenTenCodesAndAMapWithCustomerCodeAndTargetTopicWhenTheStreamReceivesTenCodeThenEveryTargetTopicShouldReceiveItsPurchaseCode() throws InterruptedException {
		// given
		String[] purchaseCodes = Accounts.accounts;
		// purchase code format: salkdjaslkdajsdlajsdklajsdaklsjdfyhbeubyhquy12345kdalsdjaksldjasldjhvbfudybdudfubdf. ascii(0-15) + Customer_Code(15-20) + ascii(20+)
		// purchases code and topic map format: { "12345" : "TOPIC_CUSTOMER_1" , "54321" : "TOPIC_CUSTOMER_2" }
		
		// when
		stream(purchaseCodes).forEach(this::sendMessage);

		// then
		assertCodeIsInTopic(purchaseCodes[0], TOPIC_CUSTOMER1);
		assertCodeIsInTopic(purchaseCodes[1], TOPIC_CUSTOMER2);
		assertCodeIsInTopic(purchaseCodes[2], TOPIC_CUSTOMER3);
		assertCodeIsInTopic(purchaseCodes[3], TOPIC_CUSTOMER3);
		assertCodeIsInTopic(purchaseCodes[4], TOPIC_CUSTOMER4);
		assertCodeIsInTopic(purchaseCodes[5], TOPIC_CUSTOMER5);
		assertCodeIsInTopic(purchaseCodes[6], TOPIC_CUSTOMER1);
		assertCodeIsInTopic(purchaseCodes[7], TOPIC_CUSTOMER2);
		assertCodeIsInTopic(purchaseCodes[8], TOPIC_CUSTOMER1);
		assertCodeIsInTopic(purchaseCodes[9], TOPIC_CUSTOMER4);
	}
	
	private void assertCodeIsInTopic(String code, String topic) {
		OutputVerifier.compareKeyValue(readMessage(topic), null, code);
	}
	
	
	@AfterEach
	public void tearDown() {
		testDriver.close();
	}
	
	private void sendMessage(final String message) {
		final KeyValue<String,String> kv = new KeyValue<String, String>(null, message);
		final List<KeyValue<String,String>> keyValues = java.util.Arrays.asList(kv);
		final List<ConsumerRecord<byte[], byte[]>> create = factory.create(keyValues);
		testDriver.pipeInput(create);
	}
	
	private ProducerRecord<String, String> readMessage(String topic) {
		return testDriver.readOutput(topic, new StringDeserializer(), new StringDeserializer());
	}

}
