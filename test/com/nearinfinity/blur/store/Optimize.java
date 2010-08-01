package com.nearinfinity.blur.store;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.store.BlurDirectory;
import com.nearinfinity.blur.lucene.store.dao.hbase.HbaseDao;

public class Optimize {

	public static void main(String[] args) throws Exception {
//		BlurDirectory directory = new BlurDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
//		FSDirectory directory = FSDirectory.open(new File("./index"));
		BlurDirectory directory = new BlurDirectory(new HbaseDao("t1", "f1", "testing"));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		final IndexWriter indexWriter = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
		indexWriter.setUseCompoundFile(false);
		long os = System.currentTimeMillis();
		indexWriter.optimize();
		System.out.println("optimize time [" + (System.currentTimeMillis() - os) + "]");
		indexWriter.close();
	}

}
