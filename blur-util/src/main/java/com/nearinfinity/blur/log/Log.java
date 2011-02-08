package com.nearinfinity.blur.log;

public interface Log extends org.apache.commons.logging.Log {
    
    public void trace(Object message, Object... args);
    public void trace(Object message, Throwable t, Object... args);
    public void debug(Object message, Object... args);
    public void debug(Object message, Throwable t, Object... args);
    public void info(Object message, Object... args);
    public void info(Object message, Throwable t, Object... args);
    public void warn(Object message, Object... args);
    public void warn(Object message, Throwable t, Object... args);
    public void error(Object message, Object... args);
    public void error(Object message, Throwable t, Object... args);
    public void fatal(Object message, Object... args);
    public void fatal(Object message, Throwable t, Object... args);

}
