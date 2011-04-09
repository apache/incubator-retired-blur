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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    private Thread readerClosingDaemon;
    private BlockingQueue<IndexReader> readersToBeClosed = new LinkedBlockingQueue<IndexReader>();
    
    public void init() throws IOException {
        setupWriter();
        commitDaemon();
    }
    
    private void commitDaemon() {
        readerClosingDaemon = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        IndexReader indexReader = readersToBeClosed.take();
                        try {
                            System.out.println("Closing [" + indexReader + "]");
                            indexReader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        readerClosingDaemon.setDaemon(true);
        readerClosingDaemon.setName("Reader Closer Thread for Directory [" + directory + "]");
        readerClosingDaemon.start();
        
        commitDaemon = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    } catch (InterruptedException e) {
                        return;
                    }
                    synchronized (writer) {
//                        System.out.println("Commiting");
                        try {
                            writer.commit();
                        } catch (IOException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                }
            }
        });
        commitDaemon.setDaemon(true);
        commitDaemon.setName("Commit Thread for Directory [" + directory + "]");
        commitDaemon.start();
    }

    private void setupWriter() throws IOException {
        writer = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
        writer.setSimilarity(new FairSimilarity());
        writer.setUseCompoundFile(false);
        ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
        mergeScheduler.setMaxThreadCount(maxThreadCountForMerger);
        writer.setMergeScheduler(mergeScheduler);
        indexReaderRef.set(writer.getReader());
        rowIndexWriter = new RowIndexWriter(writer, analyzer);
    }
    
    public void close() throws IOException {
        writer.close();
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
        try {
            readersToBeClosed.put(indexReaderRef.get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        indexReaderRef.set(reader);
    }

    public void setAnalyzer(BlurAnalyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public IndexReader getIndexReader() throws IOException {
        return indexReaderRef.get();
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }
}
