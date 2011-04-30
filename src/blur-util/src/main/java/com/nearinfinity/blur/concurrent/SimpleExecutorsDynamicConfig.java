package com.nearinfinity.blur.concurrent;

public class SimpleExecutorsDynamicConfig extends ExecutorsDynamicConfig {
    
    private int _threadSize;

    public SimpleExecutorsDynamicConfig(int threadSize) {
        _threadSize = threadSize;
    }

    @Override
    public int getCorePoolSize(String name) {
        return _threadSize;
    }

    @Override
    public long getKeepAliveTimeSeconds(String name) {
        return 60;
    }

    @Override
    public int getMaximumPoolSize(String name) {
        return _threadSize;
    }

}
