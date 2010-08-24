package org.slf4j;

import org.apache.commons.logging.LogFactory;


public class LoggerFactory {

	public static Logger getLogger(String name) {
		return new Logger(LogFactory.getLog(name));
	}

	public static Logger getLogger(Class<?> clazz) {
		return new Logger(LogFactory.getLog(clazz));
	}
}
