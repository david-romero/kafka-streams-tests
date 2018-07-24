package com.davromalc.kafka.streams.bootstrap;

import org.springframework.kafka.support.serializer.JsonSerde;

import com.davromalc.kafka.streams.model.Transaction;

public class TransactionJsonSerde extends JsonSerde<Transaction> {

	public TransactionJsonSerde() {
		super(Transaction.class);
	}
	
}
