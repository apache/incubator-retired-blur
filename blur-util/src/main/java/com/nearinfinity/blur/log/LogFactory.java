package com.nearinfinity.blur.log;

public class LogFactory {

    public static Log getLog(Class<?> c) {
        return new LogImpl(org.apache.commons.logging.LogFactory.getLog(c));
    }

}
