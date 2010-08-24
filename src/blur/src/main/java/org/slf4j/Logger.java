package org.slf4j;

import org.apache.commons.logging.Log;

public class Logger {

	private Log log;

	public Logger(Log log) {
		this.log = log;
	}

	public void info(String message, Object...args) {
		if (log.isInfoEnabled()) {
			String m = getMessage(message, args);
			Throwable throwable = getLastThrowable(args);
			if (throwable == null) {
				log.info(m);
			} else {
				log.info(m, throwable);
			}
		}
	}

	public void error(String message, Object...args) {
		if (log.isErrorEnabled()) {
			String m = getMessage(message, args);
			Throwable throwable = getLastThrowable(args);
			if (throwable == null) {
				log.error(m);
			} else {
				log.error(m, throwable);
			}
		}
	}

	public void debug(String message, Object...args) {
		if (log.isDebugEnabled()) {
			String m = getMessage(message, args);
			Throwable throwable = getLastThrowable(args);
			if (throwable == null) {
				log.debug(m);
			} else {
				log.debug(m, throwable);
			}
		}
	}
	
	private Throwable getLastThrowable(Object[] args) {
		if (args == null) {
			return null;
		}
		Object t = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof Throwable) {
				t = args[i];
			}
		}
		return (Throwable) t;
	}
	
	private String getMessage(String message, Object[] args) {
		if (args == null || args.length == 0) {
			return message;
		}
		return message;
	}

}
