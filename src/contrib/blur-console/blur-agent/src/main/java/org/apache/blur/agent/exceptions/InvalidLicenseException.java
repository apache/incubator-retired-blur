package org.apache.blur.agent.exceptions;

public class InvalidLicenseException extends Exception {
	private static final long serialVersionUID = 1317409878802094298L;

	public InvalidLicenseException(String message) {
		super(message);
	}

	public InvalidLicenseException(String message, Throwable cause) {
		super(message, cause);
	}
}
