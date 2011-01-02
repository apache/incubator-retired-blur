package com.nearinfinity.blur.utils;

import org.apache.commons.logging.Log;

import com.nearinfinity.blur.thrift.generated.BlurException;

public class LoggingBlurException extends BlurException {

    private static final long serialVersionUID = 5813322618527570189L;

    public LoggingBlurException(Log log, Exception e, String message) {
        super(message);
        log.error(message,e);
    }
}
