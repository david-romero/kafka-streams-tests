package com.davromalc.kafka.streams.bootstrap;

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

import com.davromalc.kafka.streams.logging.ReflectionException;
import com.davromalc.kafka.streams.model.Transaction;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

	@Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
	private String brokerAddresses;

	@Bean
	public ProducerFactory<String, Transaction> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}

	@Bean
	public Map<String, Object> producerConfig() {
		Map<String, Object> senderProps = KafkaTestUtils.senderProps(this.brokerAddresses);
		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // producer acks
		senderProps.put(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
		senderProps.put(ProducerConfig.RETRIES_CONFIG, "3");
		senderProps.put(ProducerConfig.LINGER_MS_CONFIG, "1");
		
        // leverage idempotent producer from Kafka 0.11 !
		//senderProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates
		
		senderProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		return senderProps;
	}

	@Bean
	public KafkaTemplate<String, Transaction> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams-once-bank");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionJsonSerde.class.getName());
		// Exactly once processing!!
		//props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		return new StreamsConfig(props);
	}

	@Bean
	public Topology bankKStreamBuilder(BankBalanceKStreamBuilder streamBuilder)
			throws ReflectionException {
		return streamBuilder.build();
	}

	@Bean
	public BankBalanceKStreamBuilder bankstreamBuilder(StreamsBuilder streamBuilder) {
		return new BankBalanceKStreamBuilder(streamBuilder, BankBalanceKStreamBuilderTest.INPUT_TOPIC,
				BankBalanceKStreamBuilderTest.OUTPUT_TOPIC);
	}

	@Bean
	Consumer<String, String> consumerInput() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumerInput-transactions");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(BankBalanceKStreamBuilderTest.INPUT_TOPIC));
		return consumer;
	}
	
	@Bean
	Consumer<String, String> consumerOutput() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumerOutput-balance");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(BankBalanceKStreamBuilderTest.OUTPUT_TOPIC));
		return consumer;
	}

}
