package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class NullReturnedException extends Exception {
	public NullReturnedException() {
		super();
	}

	public NullReturnedException(String message) {
		super(message);
	}
}
