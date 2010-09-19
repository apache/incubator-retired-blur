package com.nearinfinity.blur.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlurExecutorCompletionService<T> extends ExecutorCompletionService<T> {
    
    private AtomicInteger count = new AtomicInteger(0);

    public BlurExecutorCompletionService(Executor executor) {
        super(executor);
    }
    
    public int getRemainingCount() {
        return count.get();
    }

    @Override
    public Future<T> poll() {
        Future<T> poll = super.poll();
        if (poll != null) {
            count.decrementAndGet();
        }
        return poll;
    }

    @Override
    public Future<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
        Future<T> poll = super.poll(timeout, unit);
        if (poll != null) {
            count.decrementAndGet();
        }
        return poll;
    }

    @Override
    public Future<T> submit(Callable<T> task) {
        Future<T> future = super.submit(task);
        count.incrementAndGet();
        return future;
    }

    @Override
    public Future<T> submit(Runnable task, T result) {
        Future<T> future = super.submit(task, result);
        count.incrementAndGet();
        return future;
    }

    @Override
    public Future<T> take() throws InterruptedException {
        Future<T> take = super.take();
        if (take != null) {
            count.decrementAndGet();
        }
        return take;
    }
    
    

}
