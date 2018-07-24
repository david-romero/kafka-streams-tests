package com.davromalc.kafka.streams.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

public class Accounts {

	public static final String TOPIC = "account-input";
	public static final String TOPIC_CUSTOMER1 = "customer-1";
	public static final String TOPIC_CUSTOMER2 = "customer-2";
	public static final String TOPIC_CUSTOMER3 = "customer-3";
	public static final String TOPIC_CUSTOMER4 = "customer-4";
	public static final String TOPIC_CUSTOMER5 = "customer-5";
	
	public static final String[] accounts = new String[] {
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
	
}
