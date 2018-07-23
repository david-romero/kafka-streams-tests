package com.davromalc.kafka.streams.core.kafka.menu;

import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.davromalc.kafka.streams.model.menu.Menu;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class MenuSerde extends JsonSerde<Menu> {

	private static final ObjectMapper customObjectMapper = Jackson2ObjectMapperBuilder.json()
			.serializationInclusion(JsonInclude.Include.NON_NULL) // Don’t include null values
			.featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS) // ISODate
			.modules(new JavaTimeModule()).build();

	public MenuSerde() {
		super(Menu.class, customObjectMapper);
	}

	public MenuSerde(Class<Menu> targetType, ObjectMapper objectMapper) {
		super(Menu.class, customObjectMapper);
	}

	public MenuSerde(Class<Menu> targetType) {
		super(Menu.class, customObjectMapper);
	}

	public MenuSerde(JsonSerializer<Menu> jsonSerializer, JsonDeserializer<Menu> jsonDeserializer) {
		super(jsonSerializer, jsonDeserializer);
	}

	public MenuSerde(ObjectMapper objectMapper) {
		super(Menu.class, customObjectMapper);
	}

}
