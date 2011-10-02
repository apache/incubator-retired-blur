package com.nearinfinity.license;

public class CryptoServicesException extends Exception{
	private static final long serialVersionUID = 5526634448227766102L;

	public CryptoServicesException() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public CryptoServicesException(String message) {
        super(message);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public CryptoServicesException(String message, Throwable cause) {
        super(message, cause);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public CryptoServicesException(Throwable cause) {
        super(cause);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
