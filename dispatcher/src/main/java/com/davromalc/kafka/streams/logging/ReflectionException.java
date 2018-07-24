package com.davromalc.kafka.streams.logging;

public class ReflectionException extends Exception {

	private static final long serialVersionUID = 2410695141379387820L;

	public ReflectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ReflectionException(String message) {
		super(message);
	}

	public ReflectionException(Throwable cause) {
		super(cause);
	}
	
	

}
