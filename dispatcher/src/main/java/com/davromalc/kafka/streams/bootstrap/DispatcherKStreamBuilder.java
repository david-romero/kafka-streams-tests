package com.davromalc.kafka.streams.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.davromalc.kafka.streams.logging.LogPrinted;
import com.davromalc.kafka.streams.logging.ReflectionException;

import lombok.RequiredArgsConstructor;

@Component
public class DispatcherKStreamBuilder {

	final StreamsBuilder streamBuilder;
	final String topic;
	final Map<String, String> symbolTopicMap;
	
	final Function<String, KafkaPredicate> symbolToKafkaPredicateFuncition
	= (symbol -> new KafkaPredicate((k, v) -> v.substring(15, 20).equals(symbol)));

	@Autowired
	public DispatcherKStreamBuilder(StreamsBuilder streamBuilder,
			@Value("${kafka.topic.name}") String sourceTopic, Map<String, String> symbolTopicMap) {
		this.streamBuilder = streamBuilder;
		this.topic = sourceTopic;
		this.symbolTopicMap = symbolTopicMap;
	}

	public Topology build() throws ReflectionException {
		final KStream<String, String> stream = streamBuilder.stream(topic);

		final KStream<String, String>[] streams = stream.filter(this::hasLengthUpper20)
				.mapValues(s -> s.replace("ñ", "n")).mapValues(s -> s.startsWith("\"") ? s.substring(1) : s)
				.branch(createKafkaPredicates());

		final List<String> targetTopics = new ArrayList<>(symbolTopicMap.values());
		for (int streamIndex = 0; streamIndex < symbolTopicMap.size(); streamIndex++) {
			streams[streamIndex].to(targetTopics.get(streamIndex));
		}

		stream.print(new LogPrinted<>());

		return streamBuilder.build();

	}

	private boolean hasLengthUpper20(String key, String value) {
		return StringUtils.hasLength(value) && value.length() > 20;
	}

	private KafkaPredicate[] createKafkaPredicates() {
		final List<KafkaPredicate> predicates = symbolTopicMap.keySet().stream().map(symbolToKafkaPredicateFuncition)
				.collect(Collectors.toList());
		KafkaPredicate[] array = new KafkaPredicate[predicates.size()];
		return predicates.toArray(array);
	}
	


	@RequiredArgsConstructor
	class KafkaPredicate implements Predicate<String, String> {

		final BiPredicate<String, String> predicate;

		@Override
		public boolean test(String key, String value) {
			return predicate.test(key, value);
		}

	}

}
