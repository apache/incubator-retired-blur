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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurIndexWriter extends BlurIndex {
    
    private static final Log LOG = LogFactory.getLog(BlurIndexWriter.class);
    
    private Directory _directory;
    private IndexWriter _writer;
    private BlurAnalyzer _analyzer;
    private int _maxThreadCountForMerger = 2;
    private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
    private RowIndexWriter _rowIndexWriter;
    private Directory _sync;
    private String _name;
    private BlurIndexReaderCloser _closer;
    private BlurIndexCommiter _commiter;
    
    public void init() throws IOException {
        setupWriter();
    }
    
    private void setupWriter() throws IOException {
        _sync = watchSync(_directory);
        _name = _sync.toString();
        _writer = new IndexWriter(_sync, _analyzer, MaxFieldLength.UNLIMITED);
        _writer.setSimilarity(new FairSimilarity());
        _writer.setUseCompoundFile(false);
        ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
        mergeScheduler.setMaxThreadCount(_maxThreadCountForMerger);
        _writer.setMergeScheduler(mergeScheduler);
        _rowIndexWriter = new RowIndexWriter(_writer, _analyzer);

        _indexReaderRef.set(_writer.getReader());
        _commiter.addWriter(_name,_writer);
    }
    
    public void close() throws IOException {
        _commiter.remove(_name);
        _writer.close();
    }
    
    @Override
    public synchronized boolean replaceRow(Iterable<Row> rows) throws IOException {
        for (Row row : rows) {
            synchronized (_writer) {
                _rowIndexWriter.replace(row);
            }
        }
        rollOutNewReader(_writer.getReader());
        return true;
    }

    private synchronized void rollOutNewReader(IndexReader reader) throws IOException {
        IndexReader oldReader = _indexReaderRef.get();
        _indexReaderRef.set(reader);
        _closer.close(oldReader);
    }

    public void setAnalyzer(BlurAnalyzer analyzer) {
        _analyzer = analyzer;
    }

    @Override
    public IndexReader getIndexReader() throws IOException {
        IndexReader indexReader = _indexReaderRef.get();
        indexReader.incRef();
        return indexReader;
    }

    public void setDirectory(Directory directory) {
        this._directory = directory;
    }
    
    private Directory watchSync(Directory dir) {
        return new SyncWatcher(dir);
    }
    
    private static class SyncWatcher extends Directory {
        
        private Directory _dir;

        public SyncWatcher(Directory dir) {
            _dir = dir;
        }

        public void clearLock(String name) throws IOException {
            _dir.clearLock(name);
        }

        public void close() throws IOException {
            _dir.close();
        }

        public IndexOutput createOutput(String arg0) throws IOException {
            return _dir.createOutput(arg0);
        }

        public void deleteFile(String arg0) throws IOException {
            _dir.deleteFile(arg0);
        }

        public boolean equals(Object obj) {
            return _dir.equals(obj);
        }

        public boolean fileExists(String arg0) throws IOException {
            return _dir.fileExists(arg0);
        }

        public long fileLength(String arg0) throws IOException {
            return _dir.fileLength(arg0);
        }

        public long fileModified(String arg0) throws IOException {
            return _dir.fileModified(arg0);
        }

        public LockFactory getLockFactory() {
            return _dir.getLockFactory();
        }

        public String getLockID() {
            return _dir.getLockID();
        }

        public int hashCode() {
            return _dir.hashCode();
        }

        public String[] listAll() throws IOException {
            return _dir.listAll();
        }

        public Lock makeLock(String name) {
            return _dir.makeLock(name);
        }

        public IndexInput openInput(String name, int bufferSize) throws IOException {
            return _dir.openInput(name, bufferSize);
        }

        public IndexInput openInput(String arg0) throws IOException {
            return _dir.openInput(arg0);
        }

        public void setLockFactory(LockFactory lockFactory) throws IOException {
            _dir.setLockFactory(lockFactory);
        }

        public void sync(String name) throws IOException {
            long start = System.nanoTime();
            _dir.sync(name);
            long end = System.nanoTime();
            LOG.info("Sync of [" + name +"] took [" + (end-start) / 1000000.0 + " ms]");
        }

        public String toString() {
            return _dir.toString();
        }

        public void touchFile(String arg0) throws IOException {
            _dir.touchFile(arg0);
        }
        
    }

    public void setCommiter(BlurIndexCommiter commiter) {
        _commiter = commiter;
    }

    public void setCloser(BlurIndexReaderCloser closer) {
        _closer = closer;
    }
}
