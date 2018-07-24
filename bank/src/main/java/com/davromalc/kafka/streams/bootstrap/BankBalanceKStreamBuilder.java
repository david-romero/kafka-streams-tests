package com.davromalc.kafka.streams.bootstrap;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import com.davromalc.kafka.streams.logging.LogPrinted;
import com.davromalc.kafka.streams.logging.ReflectionException;
import com.davromalc.kafka.streams.model.Transaction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Component
public class BankBalanceKStreamBuilder {

	final StreamsBuilder streamBuilder;
	final String inputTopic;
	final String outputTopic;
	final Serde<String> keySerde = Serdes.String();
	final Serde<Balance> valueSerde = new JsonSerde<>(Balance.class);

	public BankBalanceKStreamBuilder(final StreamsBuilder streamBuilder,
			@Value(value = "${kafka.balance.topic.input.name}") final String inputTopic,
			@Value(value = "${kafka.balance.topic.output.name}") final String outputTopic) {
		this.streamBuilder = streamBuilder;
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
	}

	public KStream<String, Transaction> build() throws ReflectionException {
		final KStream<String, Transaction> stream = streamBuilder.stream(inputTopic);

		KeyValueBytesStoreSupplier supplier = new RocksDbKeyValueBytesStoreSupplier("balance-store");
		Materialized<String, Balance, KeyValueStore<Bytes, byte[]>> m = Materialized.as(supplier);  
		m = m.withKeySerde(keySerde).withValueSerde(valueSerde);
		
		stream
				.groupByKey(Serialized.with(keySerde, new TransactionJsonSerde()))
				.aggregate(() -> Balance.init(), (key, transaction, balance) -> applyTransaction(balance, transaction), m)
				.toStream()
				.to(outputTopic, Produced.with(keySerde, valueSerde));
		
		stream.print(new LogPrinted<>());

		return stream;
	}

	Balance applyTransaction(final Balance balance, final Transaction transaction) {
		final BigDecimal amount = balance.getAmount().add(BigDecimal.valueOf(transaction.getAmount())).setScale(4, RoundingMode.HALF_UP);
		final int count = balance.getTransactionCounts() + 1;
		final long timestamp = Math.max(balance.getTimestamp(), transaction.getTimestamp());
		return new Balance(amount, timestamp, count);
	}

	@Getter
	@RequiredArgsConstructor
	static class Balance {

		final BigDecimal amount;
		final long timestamp;
		final int transactionCounts;

		static Balance init() {
			return new Balance(BigDecimal.ZERO, 0, 0);
		}
		
		@JsonCreator
		static Balance fromJsonValues(@JsonProperty("amount") Double amount,@JsonProperty("timestamp") long timestamp, @JsonProperty("transactionCounts") int transactionCounts ) {
			return new Balance(BigDecimal.valueOf(amount), timestamp, transactionCounts);
		}

	}

}
