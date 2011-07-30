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

package com.nearinfinity.blur.mapreduce;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.store.WritableHdfsDirectory;
import com.nearinfinity.blur.store.cache.HdfsUtil;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.utils.Converter;
import com.nearinfinity.blur.utils.IterableConverter;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurReducer extends Reducer<BytesWritable, BlurRecord, BytesWritable, BlurRecord> {

    protected static final Field PRIME_FIELD = new Field(PRIME_DOC, PRIME_DOC_VALUE, Store.NO, Index.ANALYZED_NO_NORMS);
    protected IndexWriter _writer;
    protected Directory _directory;
    protected BlurAnalyzer _analyzer;
    protected LockFactory _lockFactory;
    protected BlurTask _blurTask;
    protected LocalFileCache _localFileCache;
    protected Counter _recordCounter;
    protected Counter _rowCounter;
    protected Counter _fieldCounter;
    protected Counter _rowBreak;
    protected Counter _rowFailures;
    protected StringBuilder _builder = new StringBuilder();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _blurTask = new BlurTask(context);

        setupCounters(context);
        setupAnalyzer(context);
        setupLockFactory(context);
        setupLocalFileCache(context);
        setupDirectory(context);

        setupWriter(context);
    }

    protected void setupCounters(Context context) {
        _rowCounter = context.getCounter(_blurTask.getCounterGroupName(), _blurTask.getRowCounterName());
        _recordCounter = context.getCounter(_blurTask.getCounterGroupName(), _blurTask.getRecordCounterName());
        _fieldCounter = context.getCounter(_blurTask.getCounterGroupName(), _blurTask.getFieldCounterName());
        _rowBreak = context.getCounter(_blurTask.getCounterGroupName(), _blurTask.getRowBreakCounterName());
        _rowFailures = context.getCounter(_blurTask.getCounterGroupName(), _blurTask.getRowFailureCounterName());
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<BlurRecord> values, Context context) throws IOException,
            InterruptedException {
        if (!index(key, values, context, false)) {
            if (!index(key, values, context, true)) {
                _rowFailures.increment(1);
            }
        }
    }

    private boolean index(BytesWritable key, Iterable<BlurRecord> values, Context context, boolean forceDelete)
            throws IOException {
        boolean primeDoc = true;
        int recordCount = 0;
        long oldRamSize = _writer.ramSizeInBytes();
        for (BlurRecord record : values) {
            Document document = toDocument(record, _builder);
            PRIMEDOC:
            if (primeDoc) {
                addPrimeDocumentField(document);
                primeDoc = false;
                if (forceDelete) {
                    delete(record.getRowId());
                } else {
                    switch (record.getOperation()) {
                    case DELETE_ROW:
                        delete(record.getRowId());
                        return true;
                    case REPLACE_ROW:
                        delete(record.getRowId());
                        break PRIMEDOC;
                    }
                }
            }
            _writer.addDocument(document);
            long newRamSize = _writer.ramSizeInBytes();
            if (newRamSize < oldRamSize) {
                _rowBreak.increment(1);
                return false;
            }
            oldRamSize = newRamSize;
            context.progress();
            recordCount++;
        }
        _recordCounter.increment(recordCount);
        _rowCounter.increment(1);
        return true;
    }

    private void delete(String rowId) throws IOException {
        _writer.deleteDocuments(new Term(ROW_ID, rowId));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        _writer.commit();
        _writer.close();
        _localFileCache.delete(HdfsUtil.getDirName(_blurTask.getTableName(), _blurTask.getShardName()));
        _localFileCache.close();
    }

    protected void setupLocalFileCache(Context context) throws IOException {
        _localFileCache = _blurTask.getLocalFileCache();
    }

    protected void setupLockFactory(Context context) throws IOException {
        //@TODO need to use zookeeper lock factory
        _lockFactory = new NoLockFactory();
    }

    protected void setupDirectory(Context context) throws IOException {
        String dirName = HdfsUtil.getDirName(nullCheck(_blurTask.getTableName()), nullCheck(_blurTask.getShardName()));
        _directory = new WritableHdfsDirectory(dirName, nullCheck(_blurTask.getDirectoryPath()),
                nullCheck(_localFileCache), nullCheck(_lockFactory), context);
    }

    protected <T> T nullCheck(T o) {
        if (o == null) {
            throw new NullPointerException();
        }
        return o;
    }

    protected void setupWriter(Context context) throws IOException {
        nullCheck(_directory);
        nullCheck(_analyzer);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_33, _analyzer);
        config.setSimilarity(new FairSimilarity());
        config.setRAMBufferSizeMB(_blurTask.getRamBufferSizeMB());
        TieredMergePolicy mergePolicy = (TieredMergePolicy) config.getMergePolicy();
        mergePolicy.setUseCompoundFile(false);
        _writer = new IndexWriter(_directory, config);
    }

    protected void setupAnalyzer(Context context) {
        _analyzer = _blurTask.getAnalyzer();
    }

    protected void addPrimeDocumentField(Document document) {
        document.add(PRIME_FIELD);
    }

    protected Document toDocument(BlurRecord record, StringBuilder builder) {
        Document document = new Document();
        document.add(new Field(ROW_ID, record.getRowId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        document.add(new Field(RECORD_ID, record.getRecordId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        String columnFamily = record.getColumnFamily();
        RowIndexWriter.addColumns(document, _analyzer, builder, columnFamily, 
            new IterableConverter<BlurColumn, Column>(
                record.getColumns(), new Converter<BlurColumn, Column>() {
                    @Override
                    public Column convert(BlurColumn from) throws Exception {
                        _fieldCounter.increment(1);
                        return new Column(from.getName(),from.getValue());
                    }
                }));
        return document;
    }
}
