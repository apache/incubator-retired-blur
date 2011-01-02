package com.nearinfinity.blur.lucene.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class MixedFSDirectoryTest {
    
    private static final String TMP_LOCALDIR = "./tmp/localdir";
    private static final String TMP_HDFSDIR = "./tmp/hdfsdir";
    private Directory directory;

    @Before
    public void setup() throws IOException {
        rm(new File("./tmp"));
        Path hdfsDirPath = new Path(TMP_HDFSDIR);
        File file = new File(TMP_LOCALDIR);
        directory = openDir(file, hdfsDirPath);
    }
    
    @Test
    public void testDir() throws CorruptIndexException, LockObtainFailedException, IOException {
        IndexWriter indexWriter = new IndexWriter(directory,new StandardAnalyzer(Version.LUCENE_30),MaxFieldLength.UNLIMITED);
        indexWriter.setUseCompoundFile(false);
        index(indexWriter);
        indexWriter.optimize();
        indexWriter.close();
        
        IndexReader indexReader = IndexReader.open(directory);
        TermEnum termEnum = indexReader.terms(new Term("f3"));
        TermDocs termDocs = null;
        while (termEnum.next()) {
            if (termDocs == null) {
                termDocs = indexReader.termDocs(termEnum.term());
            } else {
                termDocs.seek(termEnum);
            }
            while (termDocs.next()) {
                
            }
        }
        termDocs.close();
        termEnum.close();
        indexReader.close();
        
        List<String> hdfsList = getList(new File(TMP_HDFSDIR));
        List<String> localList = getList(new File(TMP_LOCALDIR));
        hdfsList.remove("segments.gen");
        localList.remove("segments.gen");
        
        assertEquals(localList,hdfsList);
    }
    
    private List<String> getList(File file) {
        return removeCrcs(new ArrayList<String>(Arrays.asList(file.list())));
    }

    private List<String> removeCrcs(List<String> list) {
        Iterator<String> iterator = list.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            if (name.endsWith(".crc")) {
                iterator.remove();
            }
        }
        return list;
    }

    private static void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    private static void index(IndexWriter indexWriter) throws CorruptIndexException, IOException {
        for (int i = 0; i < 1000; i++) {
            indexWriter.addDocument(genDoc());
        }
    }

    private static Document genDoc() {
        Document document = new Document();
        document.add(new Field("f1",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f2",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f3",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f4",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f5",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        return document;
    }

    private static Directory openDir(File file, Path hdfsDirPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        HdfsDirectory hdfsDir = new HdfsDirectory(hdfsDirPath, fileSystem);
        MixedFSDirectory directory = new MixedFSDirectory(file, new NoLockFactory());
        return new LocalReplicaDirectory(directory, hdfsDir, directory.getLockFactory());
    }

}
