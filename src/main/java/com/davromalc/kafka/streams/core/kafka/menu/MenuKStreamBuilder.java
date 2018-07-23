package com.davromalc.kafka.streams.core.kafka.menu;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import com.davromalc.kafka.streams.core.logging.LogPrinted;
import com.davromalc.kafka.streams.core.logging.ReflectionException;
import com.davromalc.kafka.streams.core.mail.MailBuilder;
import com.davromalc.kafka.streams.model.menu.Menu;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MenuKStreamBuilder {

	final StreamsBuilder streamBuilder;

	final JavaMailSender mailSender;

	final MailBuilder mailBuilder;

	final FoodReducer foodReducer;

	final String topic;

	public MenuKStreamBuilder(StreamsBuilder streamBuilder, JavaMailSender mailSender, MailBuilder mailBuilder,
			@Value(value = "${kafka.topic.name}") String topic) {
		this.streamBuilder = streamBuilder;
		this.mailSender = mailSender;
		this.mailBuilder = mailBuilder;
		this.topic = topic;
		foodReducer = new FoodReducer();
	}

	public KStream<String, Menu> build() throws ReflectionException {
		final KStream<String, Menu> stream = streamBuilder.stream(topic);
		// A hopping time window with a size of 1 days and an advance interval of 1 day.
		// the windows are aligned with epoch
		long windowSizeMs = TimeUnit.DAYS.toMillis(1);
		long advanceMs = TimeUnit.DAYS.toMillis(1);
		TimeWindows timeWindows = TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
		stream
			.groupByKey()
			.windowedBy(timeWindows)
			.reduce(foodReducer)
			.toStream()
			.peek((w, m) ->	log.info("Sending the email to {} in window start {} and end {}", m.getSubscriptor(), new Date(w.window().start()), new Date(w.window().end())))
			.mapValues(mailBuilder::build)
			.foreach((w, m) -> mailSender.send(m));
		stream.print(new LogPrinted<>());

		return stream;

	}

}
