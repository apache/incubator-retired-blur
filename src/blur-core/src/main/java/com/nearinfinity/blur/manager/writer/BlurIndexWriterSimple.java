/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurIndexWriterSimple extends BlurIndex {
    
    private Directory directory;
    private IndexWriter writer;
    private BlurAnalyzer analyzer;
    private int maxThreadCountForMerger = 5;
    private AtomicReference<IndexReader> indexReaderRef = new AtomicReference<IndexReader>();
    private RowIndexWriter rowIndexWriter;
    private Thread commitDaemon;
    private IndexReaderCloser closer = new IndexReaderCloser();
    
    public void init() throws IOException {
        setupWriter();
        commitDaemon();
        closer.init();
    }
    
    private void commitDaemon() {
//        commitDaemon = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
//                    } catch (InterruptedException e) {
//                        return;
//                    }
//                    synchronized (writer) {
//                        System.out.println("Commiting");
//                        try {
//                            writer.commit();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                            return;
//                        }
//                    }
//                }
//            }
//        });
//        commitDaemon.setDaemon(true);
//        commitDaemon.setName("Commit Thread for Directory [" + directory + "]");
//        commitDaemon.start();
    }

    private void setupWriter() throws IOException {
        writer = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
        writer.setSimilarity(new FairSimilarity());
        writer.setUseCompoundFile(false);
        ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
        mergeScheduler.setMaxThreadCount(maxThreadCountForMerger);
        writer.setMergeScheduler(mergeScheduler);
        rowIndexWriter = new RowIndexWriter(writer, analyzer);
        
        
        indexReaderRef.set(writer.getReader());
    }
    
    public void close() throws IOException {
        writer.close();
        closer.stop();
    }
    
    @Override
    public synchronized boolean replaceRow(Collection<Row> rows) throws IOException {
        synchronized (writer) {
            for (Row row : rows) {
                rowIndexWriter.replace(row);
            }
            rollOutNewReader(writer.getReader());
            return true;
        }
    }

    private void rollOutNewReader(IndexReader reader) throws IOException {
        IndexReader oldReader = indexReaderRef.get();
        indexReaderRef.set(reader);
        closer.close(oldReader);
    }

    public void setAnalyzer(BlurAnalyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public IndexReader getIndexReader() throws IOException {
        IndexReader indexReader = indexReaderRef.get();
        indexReader.incRef();
        return indexReader;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }
    
    public static class IndexReaderCloser implements Runnable {
        
        private static final Log LOG = LogFactory.getLog(IndexReaderCloser.class);
        private static final long PAUSE_TIME = TimeUnit.SECONDS.toMillis(2);
        private Thread daemon;
        private Collection<IndexReader> readers = new LinkedBlockingQueue<IndexReader>();
        private AtomicBoolean running = new AtomicBoolean();

        public void init() {
            running.set(true);
            daemon = new Thread(this);
            daemon.setDaemon(true);
            daemon.setName(getClass().getName() + "-Daemon");
            daemon.start();
        }

        public void stop() {
            running.set(false);
            daemon.interrupt();
        }

        public void close(IndexReader reader) {
            //@TODO need to a pause to the check for closed because of a race condition.
            readers.add(reader);
        }

        @Override
        public void run() {
            while (running.get()) {
                tryToCloseReaders();
                try {
                    Thread.sleep(PAUSE_TIME);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        private void tryToCloseReaders() {
            Iterator<IndexReader> it = readers.iterator();
            while (it.hasNext()) {
                IndexReader reader = it.next();
                if (reader.getRefCount() == 1) {
                    try {
                        LOG.debug("Closing indexreader [" + reader + "].");
                        reader.close();
                    } catch (IOException e) {
                        LOG.error("Error while trying to close indexreader [" + reader + "].",e);
                    }
                    it.remove();
                }
            }
        }

    }
}
