package org.apache.blur.agent.exceptions;

@SuppressWarnings("serial")
public class HdfsThreadException extends Exception {
	public HdfsThreadException() {
		super();
	}

	public HdfsThreadException(String message) {
		super(message);
	}
}
