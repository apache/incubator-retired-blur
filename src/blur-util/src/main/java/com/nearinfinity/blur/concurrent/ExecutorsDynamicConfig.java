package com.nearinfinity.blur.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ExecutorsDynamicConfig {

    private Map<String,ThreadPoolExecutor> executors = new ConcurrentHashMap<String, ThreadPoolExecutor>();

    public ThreadPoolExecutor configure(String name, ThreadPoolExecutor threadPoolExecutor) {
        executors.put(name, threadPoolExecutor);
        return threadPoolExecutor;
    }
    
    public void updateSettings() {
        for (String name : executors.keySet()) {
            ThreadPoolExecutor threadPoolExecutor = executors.get(name);
            threadPoolExecutor.setCorePoolSize(getCorePoolSize(name));
            threadPoolExecutor.setKeepAliveTime(getKeepAliveTimeSeconds(name), TimeUnit.SECONDS);
            threadPoolExecutor.setMaximumPoolSize(getMaximumPoolSize(name));
        }
    }

    public abstract int getMaximumPoolSize(String name);

    public abstract long getKeepAliveTimeSeconds(String name);

    public abstract int getCorePoolSize(String name);

}
