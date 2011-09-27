package com.nearinfinity.blur.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.store.DirectIODirectory;


public class WalIndex {
    
    private static Random random = new Random();

    public static void main(String[] args) throws IOException {
        File path = new File("./index");
//        rm(path);
        Directory dir = FSDirectory.open(path);
//        MMapDirectory dir = new MMapDirectory(path);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_33);
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_33, analyzer);
        conf.setMaxThreadStates(3);
        TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
        mergePolicy.setUseCompoundFile(false);
        WalIndexWriter writer = new WalIndexWriter(DirectIODirectory.wrap(dir), conf);
        int count = 0;
        int max = 50000;
        int size = 10;
        int total = 0;
        long s = System.nanoTime();
        long start = s;
        
        IndexReader reader = IndexReader.open(writer, true);
        System.err.println(reader.numDocs());
        
        for (int i = 0; i < 100000; i++) {
            if (count >= max) {
                long now = System.nanoTime();
                double avgSeconds = (now - start) / 1000000000.0;
                double avgRate = total / avgSeconds;
                double seconds = (now - s) / 1000000000.0;
                double rate = count / seconds;
                System.err.println(total + " at " + rate + " avg " + avgRate);
                count = 0;
                s = now;
                System.exit(0);
                writer.commitAndRollWal();
            }
//            writer.addDocuments(true,getDocs(size));
            writer.updateDocuments(true,new Term("id","id"),getDocs(size));
            total += size;
            count += size;
        }
        writer.commitAndRollWal();
        writer.close();
    }
    
    private static void rm(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    private static Collection<Document> getDocs(int size) {
        List<Document> docs = new ArrayList<Document>(size);
        for (int i = 0; i < size; i++) {
            docs.add(getDoc());
        }
        return docs;
    }

    private static Document getDoc() {
        Document document = new Document();
        long id = getId();
        document.add(new Field("id",Long.toString(id),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("value1",Long.toString(id+1),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("value2",Long.toString(id+2),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("value3",Long.toString(id+3),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("value4",Long.toString(id+4),Store.YES,Index.ANALYZED_NO_NORMS));
        return document;
    }

    private static long getId() {
        return random.nextLong();
    }

}
