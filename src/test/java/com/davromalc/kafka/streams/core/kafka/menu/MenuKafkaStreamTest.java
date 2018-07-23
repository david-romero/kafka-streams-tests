package com.davromalc.kafka.streams.core.kafka.menu;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.davromalc.kafka.streams.bootstrap.menu.KafkaStreamsConfiguration;
import com.davromalc.kafka.streams.model.menu.Menu;
import com.davromalc.kafka.streams.model.menu.MenuBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@DirtiesContext
@ContextConfiguration(classes = KafkaStreamsConfiguration.class)
@EmbeddedKafka(partitions = 3, topics = { MenuKafkaStreamTest.TOPIC})
@Slf4j
public class MenuKafkaStreamTest {

	public static final String TOPIC = "object-input-topic";

	@Autowired
	private KafkaTemplate<String, Menu> template;

	@Autowired
	private JavaMailSender javaMailSender;
	
	@Autowired
	Consumer<String, String> consumer;

	private final Executor executor = Executors.newCachedThreadPool();
	
	private final Duration SIX_MINUTES = new Duration(6, TimeUnit.MINUTES);

	@Test
	@DisplayName("Given An Only Customer and Many Messages When Stream Is Invoked Then a Single Email Must Be Sent")
	public void givenAOnlyCustomerAndManyMessagesWhenStreamIsInvokedThenASingleEmailMustBeSent() throws InterruptedException {
		executor.execute(new MenuKafkaSender(200, 1));

		Awaitility.await().atMost(SIX_MINUTES).pollInterval(Duration.FIVE_SECONDS).until(() -> {
			int messages = consumer.poll(1000).count();
			int emailsSent =  BDDMockito.mockingDetails(javaMailSender).getInvocations().size();
			log.info("We have {} messages and we have sent {} emails", messages, emailsSent);
			return messages == 0 &&  emailsSent > 0;
		});
		
		BDDMockito.verify(javaMailSender, BDDMockito.times(1)).send(BDDMockito.any(MimeMessagePreparator.class));
	}
	
	@Test
	@DisplayName("Given Two Customers and Many Messages When Stream Is Invoked Then Two Emails Must Be Sent")
	public void givenTwoCustomersAndManyMessagesWhenStreamIsInvokedThenTwoEmailsMustBeSent() throws InterruptedException {
		//given
		BDDMockito.reset(javaMailSender);
		executor.execute(new MenuKafkaSender(200, 2));

		// when
		Awaitility.await().atMost(SIX_MINUTES).pollInterval(Duration.FIVE_SECONDS).until(() -> {
			int messages = consumer.poll(1000).count();
			int emailsSent =  BDDMockito.mockingDetails(javaMailSender).getInvocations().size();
			log.info("We have {} messages and we have sent {} emails", messages, emailsSent);
			return messages == 0 &&  emailsSent > 0;
		});
		
		// when
		BDDMockito.verify(javaMailSender, BDDMockito.times(2)).send(BDDMockito.any(MimeMessagePreparator.class));
	}



	
	@RequiredArgsConstructor
	private class MenuKafkaSender implements Runnable {
		
		final int messageNumber;
		final int customerNumber;
		
		public void run() {
			IntStream.range(0, customerNumber).mapToObj(customerNumber -> UUID.randomUUID().toString() + "-" + customerNumber).forEach(customerKey -> {
				IntStream.range(0, messageNumber).forEach(i -> {
					Menu menu = new MenuBuilder()
							.withSubscriptor("David Romero", "admin@example.com")
							.build();
					template.send(TOPIC, customerKey, menu);
					sleep();
				});
			});
		}
		
		private void sleep() {
			try {
				Thread.sleep(ThreadLocalRandom.current().nextLong(50, 1000));
			} catch (InterruptedException ignore) {	}
		}
		
	}
	
	
}
