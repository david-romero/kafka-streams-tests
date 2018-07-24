package com.davromalc.kafka.streams.logging;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.kafka.streams.kstream.Printed;


public class LogPrinted<T> extends Printed<String, T> {

	
	public LogPrinted() throws ReflectionException {
		super(Printed.toSysOut());
		try {
			Field field = Printed.class.getDeclaredField("printWriter");
			field.setAccessible(true);
			Field modifiersField = Field.class.getDeclaredField("modifiers");
			modifiersField.setAccessible(true);
			modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
			field.set(this, new LogPrintWriter("kafka-stream", System.out));
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw new ReflectionException(e);
		}
	}

}
