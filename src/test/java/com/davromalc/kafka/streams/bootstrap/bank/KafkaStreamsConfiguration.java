package com.davromalc.kafka.streams.bootstrap.bank;

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
import org.apache.kafka.streams.kstream.KStream;
import org.mockito.BDDMockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mail.javamail.JavaMailSender;

import com.davromalc.kafka.streams.core.kafka.bank.BankBalanceKStreamBuilder;
import com.davromalc.kafka.streams.core.kafka.bank.BankBalanceKStreamBuilderTest;
import com.davromalc.kafka.streams.core.logging.ReflectionException;
import com.davromalc.kafka.streams.model.bank.Transaction;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafkaStreams
@Slf4j
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
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder(StreamsConfig kStreamsConfigs) {
		StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(kStreamsConfigs);
	    streamsBuilderFactoryBean.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
	        @Override
	        public void uncaughtException(Thread t, Throwable e) {
	            log.warn("StopStartStreamsUncaughtExceptionHandler caught exception {}, stopping StreamsThread ..", e);
	            streamsBuilderFactoryBean.stop();
	        }
	    });
	    return streamsBuilderFactoryBean;
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
	public KStream<String, Transaction> bankKStreamBuilder(BankBalanceKStreamBuilder streamBuilder)
			throws ReflectionException {
		return streamBuilder.build();
	}

	@Bean
	public BankBalanceKStreamBuilder bankstreamBuilder(StreamsBuilder streamBuilder) {
		return new BankBalanceKStreamBuilder(streamBuilder, BankBalanceKStreamBuilderTest.INPUT_TOPIC,
				BankBalanceKStreamBuilderTest.INPUT_TOPIC);
	}

	@Bean
	JavaMailSender mailSender() {
		return BDDMockito.mock(JavaMailSender.class);
	}

	@Bean
	Consumer<String, String> consumerInput() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumerInput");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumerOutput");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(BankBalanceKStreamBuilderTest.OUTPUT_TOPIC));
		return consumer;
	}

}
