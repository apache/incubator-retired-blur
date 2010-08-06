package com.nearinfinity.blur.store;

import java.io.File;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Optimize {

	public static void main(String[] args) throws Exception {
		FSDirectory directory = FSDirectory.open(new File("./index"));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		final IndexWriter indexWriter = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
		indexWriter.setUseCompoundFile(false);
		long os = System.currentTimeMillis();
		indexWriter.optimize();
		System.out.println("optimize time [" + (System.currentTimeMillis() - os) + "]");
		indexWriter.close();
	}

}
