package com.davromalc.kafka.streams.bootstrap;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import com.davromalc.kafka.streams.logging.ReflectionException;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	@Value(value = "${kafka.groupId.key}")
	private String groupIdKey;

	@Value(value = "${kafka.groupId.defaultValue}")
	private String groupIdDefaultValue;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs(Environment env) {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty(groupIdKey, groupIdDefaultValue));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return new StreamsConfig(props);
	}

	@Bean
	public Topology kStream(final DispatcherKStreamBuilder kStreamBuilder)
			throws ReflectionException {
		return kStreamBuilder.build();
	}
	
	@Bean
	Map<String,String> customerMap(){
		Map<String,String> map = new HashMap<>(2);
		map.put("12345678Z", "customer1-topic");
		map.put("87654321A", "customer2-topic");
		return map;
	}

}
