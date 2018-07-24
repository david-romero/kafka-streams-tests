package com.davromalc.kafka.streams.bootstrap;


import static com.davromalc.kafka.streams.model.Accounts.SYMBOL_TOPIC_MAP;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER1;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER2;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER3;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER4;
import static com.davromalc.kafka.streams.model.Accounts.TOPIC_CUSTOMER5;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.davromalc.kafka.streams.bootstrap.DispatcherKStreamBuilder;
import com.davromalc.kafka.streams.logging.ReflectionException;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

	@Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
	private String brokerAddresses;

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(this.brokerAddresses);
		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return senderProps;
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams-dispatcher");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		return new StreamsConfig(props);
	}

	@Bean
	public Topology dispatcherKStream(DispatcherKStreamBuilder streamBuilder)
			throws ReflectionException {
		return streamBuilder.build();
	}

	@Bean
	public DispatcherKStreamBuilder streamBuilder(StreamsBuilder streamBuilder) {
		return new DispatcherKStreamBuilder(streamBuilder, TOPIC,
				SYMBOL_TOPIC_MAP);
	}

	@Bean
	Consumer<String, String> consumerTopic() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-dispatcher");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	@Bean
	Consumer<String, String> consumerCustomerTopic1() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-customer1");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC_CUSTOMER1));
		return consumer;
	}
	
	@Bean
	Consumer<String, String> consumerCustomerTopic2() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-customer2");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC_CUSTOMER2));
		return consumer;
	}
	
	@Bean
	Consumer<String, String> consumerCustomerTopic3() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-customer3");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC_CUSTOMER3));
		return consumer;
	}
	
	@Bean
	Consumer<String, String> consumerCustomerTopic4() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-customer4");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC_CUSTOMER4));
		return consumer;
	}
	
	@Bean
	Consumer<String, String> consumerCustomerTopic5() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-customer5");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC_CUSTOMER5));
		return consumer;
	}

}
