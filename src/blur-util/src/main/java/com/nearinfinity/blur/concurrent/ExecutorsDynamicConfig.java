package com.nearinfinity.blur.concurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ExecutorsDynamicConfig {

    private ThreadPoolExecutor threadPoolExecutor;
    private String name;

    public ThreadPoolExecutor configure(String name, ThreadPoolExecutor threadPoolExecutor) {
        this.name = name;
        this.threadPoolExecutor = threadPoolExecutor;
        return threadPoolExecutor;
    }
    
    public void updateSettings() {
        threadPoolExecutor.setCorePoolSize(getCorePoolSize());
        threadPoolExecutor.setKeepAliveTime(getKeepAliveTimeSeconds(), TimeUnit.SECONDS);
        threadPoolExecutor.setMaximumPoolSize(getMaximumPoolSize());
    }

    public abstract int getMaximumPoolSize();

    public abstract long getKeepAliveTimeSeconds();

    public abstract int getCorePoolSize();

    public String getName() {
        return name;
    }

}
