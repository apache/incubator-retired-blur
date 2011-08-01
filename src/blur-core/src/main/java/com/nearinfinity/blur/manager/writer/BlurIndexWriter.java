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

import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurIndexWriter extends BlurIndex {
    
    private static final Log LOG = LogFactory.getLog(BlurIndexWriter.class);
    
    private Directory _directory;
    private IndexWriter _writer;
    private BlurAnalyzer _analyzer;
    private AtomicReference<IndexReader> _indexReaderRef = new AtomicReference<IndexReader>();
    private Directory _sync;
    private BlurIndexCloser _closer;
    private BlurIndexRefresher _refresher;
    private RowIndexWriter _rowIndexWriter;
    private BlurIndexCommiter _commiter;
    private AtomicBoolean _open = new AtomicBoolean();
    private String _id = UUID.randomUUID().toString();
    
    public void init() throws IOException {
        _sync = watchSync(_directory);
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_33, _analyzer);
        conf.setSimilarity(new FairSimilarity());
        TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
        mergePolicy.setUseCompoundFile(false);
        _writer = new IndexWriter(_sync, conf);
        _indexReaderRef.set(IndexReader.open(_writer, true));
        _rowIndexWriter = new RowIndexWriter(_writer, _analyzer);
        _open.set(true);
        _refresher.register(this);
        _commiter.addWriter(_id, _writer);
    }
    
    @Override
    public void refresh() throws IOException {
        if (!_open.get()) {
            return;
        }
        IndexReader oldReader = _indexReaderRef.get();
        if (oldReader.isCurrent()) {
            return;
        }
        synchronized (_writer) {
            try {
                IndexReader reader = oldReader.reopen(_writer, true);
                if (oldReader != reader) {
                    _indexReaderRef.set(reader);
                    _closer.close(oldReader);
                }
            } catch (AlreadyClosedException e) {
                LOG.warn("Writer was already closed, this can happen during closing of a writer.");
            }
        }
    }
    
    @Override
    public IndexReader getIndexReader(boolean forceRefresh) throws IOException {
        if (forceRefresh) {
            refresh();
        }
        IndexReader indexReader = _indexReaderRef.get();
        indexReader.incRef();
        return indexReader;
    }
    
    @Override
    public void close() throws IOException {
        _commiter.remove(_id);
        _open.set(false);
        _refresher.unregister(this);
        _writer.close();
    }
    
    @Override
    public synchronized boolean replaceRow(Row row) throws IOException {
        _rowIndexWriter.replace(row);
        return true;
    }
    
    @Override
    public void deleteRow(String rowId) throws IOException {
        synchronized (_writer) {
            _writer.deleteDocuments(new Term(ROW_ID,rowId));
        }
    }
    
    public void setAnalyzer(BlurAnalyzer analyzer) {
        _analyzer = analyzer;
    }

    public void setDirectory(Directory directory) {
        _directory = directory;
    }
    
    private Directory watchSync(Directory dir) {
        return new SyncWatcher(dir);
    }
    
    public void setCloser(BlurIndexCloser closer) {
        _closer = closer;
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

        @SuppressWarnings("deprecation")
        public void sync(String name) throws IOException {
            long start = System.nanoTime();
            _dir.sync(name);
            long end = System.nanoTime();
            LOG.debug("Sync of [" + name +"] took [" + (end-start) / 1000000.0 + " ms]");
        }

        public String toString() {
            return _dir.toString();
        }

        @SuppressWarnings("deprecation")
        public void touchFile(String arg0) throws IOException {
            _dir.touchFile(arg0);
        }
    }

    public void setRefresher(BlurIndexRefresher refresher) {
        _refresher = refresher;
    }

    public void setCommiter(BlurIndexCommiter commiter) {
        _commiter = commiter;
    }

}
