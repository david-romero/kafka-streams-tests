package com.davromalc.kafka.streams.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.davromalc.kafka.streams.model.Transaction;

import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = KafkaStreamsConfiguration.class)
@EmbeddedKafka(partitions = 1, topics = { BankBalanceKStreamBuilderTest.INPUT_TOPIC,
		BankBalanceKStreamBuilderTest.OUTPUT_TOPIC })
@Slf4j
public class BankBalanceKStreamBuilderTest {

	public static final String INPUT_TOPIC = "input";
	public static final String OUTPUT_TOPIC = "output";

	@Autowired
	private KafkaTemplate<String, Transaction> template;

	@Autowired
	Consumer<String, String> consumerInput;

	@Autowired
	Consumer<String, String> consumerOutput;

	private final Executor executor = Executors.newCachedThreadPool();

	String[] names = new String[] { "David", "John", "Manuel", "Carl" };

	@Test
	@DisplayName("Given A Large Number Of Transactions In Concurrent Mode When The Stream Process All Messages Then All Balances Should Be Calculated")
	public void givenALargeNumberOfTransactionsInConcurrentModeWhenTheStreamProcessAllMessagesThenAllBalancesShouldBeCalculated()
			throws InterruptedException {
		int numberOfTransactions = 600;
		executor.execute(() -> {
			for (int i = 0; i < numberOfTransactions; i++) {
				Transaction t = new Transaction();
				t.setAmount(ThreadLocalRandom.current().nextDouble(0.0, 100.0));
				final String name = names[ThreadLocalRandom.current().nextInt(0, 4)];
				t.setName(name);
				t.setTimestamp(System.nanoTime());
				template.send(INPUT_TOPIC, name, t);
			}
		});

		Awaitility.await().atMost(Duration.FIVE_MINUTES).pollInterval(new Duration(5, TimeUnit.SECONDS)).until(() -> {
			int messages = consumerInput.poll(1000).count();
			log.info("We have {} messages", messages);
			return messages == 0;
		});

		assertEquals(4, consumerOutput.poll(1000).count());
	}

}
