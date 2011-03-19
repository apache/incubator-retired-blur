package com.nearinfinity.blur.concurrent;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class SimpleUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Log LOG = LogFactory.getLog(SimpleUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Unknown error in thread [{0}]", e, t);
    }
}
