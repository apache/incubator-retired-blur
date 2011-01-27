package com.nearinfinity.blur.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.store.HdfsUtil;
import com.nearinfinity.blur.store.LocalFileCache;
import com.nearinfinity.blur.store.WritableHdfsDirectory;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurReducer extends Reducer<BytesWritable,BlurRecord,BytesWritable,BlurRecord> implements BlurConstants {
    
    protected static final Field PRIME_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.ANALYZED_NO_NORMS);
    protected IndexWriter writer;
    protected Directory directory;
    protected BlurAnalyzer analyzer;
    protected IndexDeletionPolicy deletionPolicy;
    protected LockFactory lockFactory;
    protected IndexCommit commitPoint;
    protected BlurTask blurTask;
    protected FileSystem fileSystem;
    protected LocalFileCache localFileCache;
    protected Counter recordCounter;
    protected Counter rowCounter;
    protected Counter fieldCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        blurTask = new BlurTask(context);

        setupCounters(context);
        setupAnalyzer(context);
        setupFileSystem(context);
        setupLockFactory(context);
        setupLocalFileCache(context);
        setupDirectory(context);
        
        setupIndexDeletionPolicy(context);
        setupEmptyIndex(context);
        setupCommitPoint(context);
        setupWriter(context);
    }
    
    protected void setupCounters(Context context) {
        rowCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRowCounterName());
        recordCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRecordCounterName());
        fieldCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getFieldCounterName());
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<BlurRecord> values, Context context) throws IOException, InterruptedException {
        boolean primeDoc = true;
        for (BlurRecord record : values) {
            Document document = toDocument(record);
            PRIMEDOC:
            if (primeDoc) {
                addPrimeDocumentField(document);
                primeDoc = false;
                switch (record.getOperation()) {
                case DELETE_ROW:
                    writer.deleteDocuments(new Term(ID,record.getId()));
                    return;
                case REPLACE_ROW:
                    writer.deleteDocuments(new Term(ID,record.getId()));
                    break PRIMEDOC;
                }
            }
            writer.addDocument(document);
            context.progress();
            recordCounter.increment(1);
        }
        rowCounter.increment(1);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writer.optimize();
        writer.commit(blurTask.getCommitUserData());
        writer.close();
        localFileCache.delete(HdfsUtil.getDirName(blurTask.getTableName(), blurTask.getShardName()));
        localFileCache.close();
    }
    
    protected void setupLocalFileCache(Context context) throws IOException {
        localFileCache = blurTask.getLocalFileCache();
    }

    protected void setupFileSystem(Context context) throws IOException {
        fileSystem = FileSystem.get(context.getConfiguration());
    }

    protected void setupLockFactory(Context context) throws IOException {
        //need to use zookeeper lock factory
        lockFactory = new NoLockFactory();
    }
    
    protected void setupDirectory(Context context) throws IOException {
        directory = new WritableHdfsDirectory(nullCheck(blurTask.getTableName()), nullCheck(blurTask.getShardName()), 
                nullCheck(blurTask.getDirectoryPath()), nullCheck(fileSystem), nullCheck(localFileCache), 
                nullCheck(lockFactory),context);
    }

    protected <T> T nullCheck(T o) {
        if (o == null) {
            throw new NullPointerException();
        }
        return o;
    }
    
    protected void setupEmptyIndex(Context context) throws IOException {
        nullCheck(directory);
        if (!IndexReader.indexExists(directory)) {
            IndexWriter indexWriter = new IndexWriter(nullCheck(directory), nullCheck(analyzer), 
                    nullCheck(deletionPolicy), MaxFieldLength.UNLIMITED);
            indexWriter.setUseCompoundFile(false);
            indexWriter.addDocument(new Document());//hack to make empty commit point stick
            indexWriter.commit(blurTask.getEmptyCommitUserData());
            indexWriter.close();
        }        
    }

    protected void setupWriter(Context context) throws IOException {
        writer = new IndexWriter(nullCheck(directory), nullCheck(analyzer), 
                nullCheck(deletionPolicy), MaxFieldLength.UNLIMITED, nullCheck(commitPoint));
        writer.setRAMBufferSizeMB(blurTask.getRamBufferSizeMB());
        writer.setUseCompoundFile(false);
        writer.setSimilarity(new FairSimilarity());
    }
    
    protected void setupAnalyzer(Context context) {
        analyzer = blurTask.getAnalyzer();
    }
    
    protected void setupCommitPoint(Context context) throws IOException {
        commitPoint = blurTask.getIndexCommitPointNameToOpen(IndexReader.listCommits(directory));
    }
    
    protected void setupIndexDeletionPolicy(Context context) {
        deletionPolicy = new IndexDeletionPolicy() {
            @Override
            public void onInit(List<? extends IndexCommit> commits) throws IOException {
                //keep everything
            }
            @Override
            public void onCommit(List<? extends IndexCommit> commits) throws IOException {
                //keep everything
            }
        };
    }
    
    protected void addPrimeDocumentField(Document document) {
        document.add(PRIME_FIELD);
    }

    protected Document toDocument(BlurRecord record) {
        Document document = new Document();
        document.add(new Field(ID, record.getId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        document.add(new Field(SUPER_KEY, record.getSuperKey(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        String columnFamily = record.getColumnFamily();
        for (BlurColumn column : record.getColumns()) {
            addField(columnFamily,document,column);
            fieldCounter.increment(1);
        }
        return document;
    }

    protected void addField(String columnFamily, Document document, BlurColumn column) {
        String indexName = columnFamily + "." + column.getName();
        document.add(new Field(indexName,column.getValue(),Store.YES,Index.ANALYZED_NO_NORMS));
        Set<String> subIndexNames = analyzer.getSubIndexNames(indexName);
        if (subIndexNames == null) {
            return;
        }
        for (String subIndexName : subIndexNames) {
            document.add(new Field(subIndexName,column.getValue(),Store.NO,Index.ANALYZED_NO_NORMS));
        }
    }
}
