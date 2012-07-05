package com.nearinfinity.blur.jdbc.util;

public class NotImplemented extends RuntimeException {

    public static final String BLUR_JDBC_DEBUG = "blur.jdbc.debug";
    private static final long serialVersionUID = 4736975316647139778L;
    public static final boolean debug = Boolean.getBoolean(BLUR_JDBC_DEBUG);
    
    public NotImplemented() {
        this(null);
    }
    
    public NotImplemented(String name) {
        if (debug) {
            if (name != null) {
                System.err.println("Method [" + name + "]");
            }
            new Throwable().printStackTrace();
        }
    }
}
