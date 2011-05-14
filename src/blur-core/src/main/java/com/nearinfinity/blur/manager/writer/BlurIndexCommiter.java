package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexWriter;

public class BlurIndexCommiter {
    
    private static final Log LOG = LogFactory.getLog(BlurIndexCommiter.class);

    private Thread commitDaemon;
    private Map<String,IndexWriter> writers = new ConcurrentHashMap<String, IndexWriter>();
    private AtomicBoolean running = new AtomicBoolean();
    
    public void init() {
        running.set(true);
        commitDaemon = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    commit();
                    try {
                        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        commitDaemon.setDaemon(true);
        commitDaemon.setName("Commit Thread");
        commitDaemon.start();
    }

    public void commit() {
        for (String name : writers.keySet()) {
            IndexWriter writer = writers.get(name);
            if (writer != null) {
                commitIndex(name, writer);
            }
        }
    }

    private void commitIndex(String name, IndexWriter writer) {
        synchronized (writer) {
            LOG.info("Commiting writer for [" + name + "]");
            try {
                writer.commit();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public void addWriter(String name, IndexWriter writer) {
        writers.put(name, writer);
    }

    public void close() {
        running.set(false);
        commitDaemon.interrupt();
        commit();
        try {
            commitDaemon.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void remove(String name) {
        IndexWriter writer = writers.remove(name);
        commitIndex(name, writer);
    }

}
