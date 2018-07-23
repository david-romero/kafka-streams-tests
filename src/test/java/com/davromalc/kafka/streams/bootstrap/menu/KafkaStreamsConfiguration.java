package com.davromalc.kafka.streams.bootstrap.menu;

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
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mail.javamail.JavaMailSender;
import org.thymeleaf.TemplateEngine;

import com.davromalc.kafka.streams.core.kafka.menu.MenuKStreamBuilder;
import com.davromalc.kafka.streams.core.kafka.menu.MenuKafkaStreamTest;
import com.davromalc.kafka.streams.core.kafka.menu.MenuSerde;
import com.davromalc.kafka.streams.core.logging.ReflectionException;
import com.davromalc.kafka.streams.core.mail.MailBuilder;
import com.davromalc.kafka.streams.model.menu.Menu;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

	@Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
	private String brokerAddresses;

	@Bean
	public ProducerFactory<String, Menu> producerFactory() {
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
	public KafkaTemplate<String, Menu> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams-menu");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "240000"); // cada 4 minutos commitea
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MenuSerde.class.getName());
		return new StreamsConfig(props);
	}

	@Bean
	public KStream<String, Menu> kStreamMenu(MenuKStreamBuilder streamBuilder) throws ReflectionException {
		return streamBuilder.build();
	}

	@Bean
	public MenuKStreamBuilder streamBuilder(StreamsBuilder streamBuilder, JavaMailSender mailSender) {
		return new MenuKStreamBuilder(streamBuilder, mailSender, new MailBuilder(BDDMockito.mock(TemplateEngine.class)),
				MenuKafkaStreamTest.TOPIC);
	}

	@Bean
	JavaMailSender mailSender() {
		return BDDMockito.mock(JavaMailSender.class);
	}

	@Bean
	Consumer<String, String> menuConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer-topic-menu");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(MenuKafkaStreamTest.TOPIC));
		return consumer;
	}

}
