package com.nearinfinity.blur.search;

import java.io.IOException;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.index.SuperDocument;
import com.nearinfinity.blur.index.SuperIndexReader;

public class SuperSearcher extends IndexSearcher {

	public SuperSearcher(Directory path) throws CorruptIndexException, IOException {
		super(new SuperIndexReader(IndexReader.open(path, true)));
	}
	
	public SuperSearcher(SuperIndexReader r) throws IOException {
		super(r);
	}

	public SuperSearcher(IndexReader r) throws IOException {
		super(new SuperIndexReader(r));
	}

	public SuperDocument superDoc(int i) throws CorruptIndexException, IOException {
		return null;
	}

	public SuperDocument superDoc(int i, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
		return null;
	}
	
}
