package com.nearinfinity.blur.mapreduce;

import java.io.IOException;
import java.util.List;

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
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.store.LocalFileCache;
import com.nearinfinity.blur.store.WritableHdfsDirectory;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurReducer extends Reducer<BytesWritable,BlurRecord,BytesWritable,BlurRecord> implements BlurConstants {
    
    private static final Field PRIME_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.ANALYZED_NO_NORMS);
    private IndexWriter writer;
    private Directory directory;
    private BlurAnalyzer analyzer;
    private IndexDeletionPolicy deletionPolicy;
    private LockFactory lockFactory;
    private IndexCommit commitPoint;
    private BlurTask blurTask;
    private FileSystem fileSystem;
    private LocalFileCache localFileCache;
    private Counter recordCounter;
    private Counter rowCounter;
    private Counter fieldCounter;
    
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
    
    private void setupCounters(Context context) {
        rowCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRowCounterName());
        recordCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRecordCounterName());
        fieldCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getFieldCounterName());
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<BlurRecord> values, Context context) throws IOException, InterruptedException {
        boolean primeDoc = true;
        for (BlurRecord record : values) {
            Document document = toDocument(record);
            if (primeDoc) {
                addPrimeDocumentField(document);
                primeDoc = false;
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
    }
    
    private void setupLocalFileCache(Context context) throws IOException {
        localFileCache = blurTask.getLocalFileCache();
    }

    private void setupFileSystem(Context context) throws IOException {
        fileSystem = FileSystem.get(context.getConfiguration());
    }

    private void setupLockFactory(Context context) throws IOException {
        //need to use zookeeper lock factory
        lockFactory = new NoLockFactory();
    }
    
    private void setupDirectory(Context context) throws IOException {
        directory = new WritableHdfsDirectory(nullCheck(blurTask.getDirectoryName()), nullCheck(blurTask.getDirectoryPath()),
                nullCheck(fileSystem), nullCheck(localFileCache), nullCheck(lockFactory));
    }

    private <T> T nullCheck(T o) {
        if (o == null) {
            throw new NullPointerException();
        }
        return o;
    }
    
    private void setupEmptyIndex(Context context) throws IOException {
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

    private void setupWriter(Context context) throws IOException {
        writer = new IndexWriter(nullCheck(directory), nullCheck(analyzer), 
                nullCheck(deletionPolicy), MaxFieldLength.UNLIMITED, nullCheck(commitPoint));
        writer.setRAMBufferSizeMB(blurTask.getRamBufferSizeMB());
        writer.setMergeFactor(Integer.MAX_VALUE);//never merge while indexing, this is done during optimize
        writer.setUseCompoundFile(false);
        writer.setSimilarity(new FairSimilarity());
    }
    
    private void setupAnalyzer(Context context) {
        analyzer = blurTask.getAnalyzer();
    }
    
    private void setupCommitPoint(Context context) throws IOException {
        commitPoint = blurTask.getIndexCommitPointNameToOpen(IndexReader.listCommits(directory));
    }
    
    private void setupIndexDeletionPolicy(Context context) {
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
    
    private void addPrimeDocumentField(Document document) {
        document.add(PRIME_FIELD);
    }

    private Document toDocument(BlurRecord record) {
        Document document = new Document();
        document.add(new Field(ID, record.getId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        document.add(new Field(SUPER_KEY, record.getSuperKey(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        for (BlurColumn column : record.getColumns()) {
            addField(document,column);
            fieldCounter.increment(1);
        }
        return document;
    }

    private void addField(Document document, BlurColumn column) {
        document.add(new Field(column.getName(),column.getValue(),Store.YES,Index.ANALYZED_NO_NORMS));
    }
}
