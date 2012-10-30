package com.nearinfinity.license.utils;

public class CryptoServicesException extends Exception {
	private static final long serialVersionUID = 5526634448227766102L;

	public CryptoServicesException() {
		super();
	}

	public CryptoServicesException(String message) {
		super(message);
	}

	public CryptoServicesException(String message, Throwable cause) {
		super(message, cause);
	}

	public CryptoServicesException(Throwable cause) {
		super(cause);
	}
}
