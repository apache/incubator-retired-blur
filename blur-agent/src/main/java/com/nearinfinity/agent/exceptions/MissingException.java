package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class MissingException extends Exception {
	public MissingException(String type, String name) {
		super("Couldn't Find a " + type + " by name [" + name + "].  Need a single result.  Skipping collection.");
	}
}
