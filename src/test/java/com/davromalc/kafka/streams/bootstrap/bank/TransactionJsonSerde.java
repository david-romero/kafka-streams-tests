package com.davromalc.kafka.streams.bootstrap.bank;

import org.springframework.kafka.support.serializer.JsonSerde;

import com.davromalc.kafka.streams.model.bank.Transaction;

public class TransactionJsonSerde extends JsonSerde<Transaction> {

	public TransactionJsonSerde() {
		super(Transaction.class);
	}
	
}
