package com.davromalc.kafka.streams.model;

import lombok.Data;

@Data
public class Transaction {

	String name;
	Double amount;
	long timestamp;
	
}
