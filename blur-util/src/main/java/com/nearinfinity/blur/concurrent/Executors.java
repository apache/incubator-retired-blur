package com.nearinfinity.blur.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Executors {

    public static ExecutorService newCachedThreadPool(String prefix) {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, 
                new SynchronousQueue<Runnable>(), new BlurThreadFactory(prefix));
    }

    public static class BlurThreadFactory implements ThreadFactory {
        private AtomicInteger threadNumber = new AtomicInteger(0);
        private String prefix;

        public BlurThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(prefix + threadNumber.getAndIncrement());
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            return t;
        }
    }
    
    private Executors() {
        
    }
}
