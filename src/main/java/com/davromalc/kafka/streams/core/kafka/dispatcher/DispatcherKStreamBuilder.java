package com.davromalc.kafka.streams.core.kafka.dispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.davromalc.kafka.streams.core.logging.LogPrinted;
import com.davromalc.kafka.streams.core.logging.ReflectionException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DispatcherKStreamBuilder {

	final StreamsBuilder streamBuilder;
	final String topic;
	final Map<String, String> symbolTopicMap;

	@Autowired
	public DispatcherKStreamBuilder(StreamsBuilder streamBuilder, String sourceTopic,
			Map<String, String> symbolTopicMap) {
		this.streamBuilder = streamBuilder;
		this.topic = sourceTopic;
		this.symbolTopicMap = symbolTopicMap;
	}

	public Topology build() throws ReflectionException {
		final KStream<String, String> stream = streamBuilder.stream(topic);
		
		final KStream<String, String>[] streams =  stream
			.filter((k , v) -> StringUtils.length(v) > 20)
			.mapValues(s -> s.replace("ñ", "n"))
			.mapValues(s -> s.startsWith("\"") ? s.substring(1) : s)
			.branch(createPredicates());
		
		final List<String> targetTopics = new ArrayList<>(symbolTopicMap.values());
		for (int streamIndex = 0;streamIndex < symbolTopicMap.size(); streamIndex ++) {
			streams[streamIndex].to(targetTopics.get(streamIndex));
		}
		
		stream.print(new LogPrinted<>());
		
		return streamBuilder.build();

	}

	private CustomPredicate[] createPredicates() {
		List<CustomPredicate> predicates = symbolTopicMap.keySet().stream().map(new SymbolFilterFunction()).collect(Collectors.toList());
		CustomPredicate[] array = new CustomPredicate[predicates.size()];
		return predicates.toArray(array);
	}
	
	
	class SymbolFilterFunction implements Function<String, CustomPredicate>{

		@Override
		public CustomPredicate apply(final String symbol) {
			log.info("symbol:" + symbol);
			return new CustomPredicate((k , v) -> v.substring(15, 20).equals(symbol));
		}
		
	}
	
	@RequiredArgsConstructor
	class CustomPredicate implements Predicate<String, String> {

		final BiPredicate<String, String> predicate;
		
		@Override
		public boolean test(String key, String value) {
			return predicate.test(key, value);
		}
		
	}
	
	
}
