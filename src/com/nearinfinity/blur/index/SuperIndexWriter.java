package com.nearinfinity.blur.index;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;


public class SuperIndexWriter extends IndexWriter {
	
	static final Fieldable PRIME_DOC_FIELD = new Field(SuperDocument.PRIME_DOC, SuperDocument.PRIME_DOC_VALUE, Store.NO, Index.ANALYZED_NO_NORMS);

	public SuperIndexWriter(Directory d, Analyzer a, boolean create, IndexDeletionPolicy deletionPolicy,
			MaxFieldLength mfl) throws CorruptIndexException, LockObtainFailedException, IOException {
		super(d, a, create, deletionPolicy, mfl);
	}

	public SuperIndexWriter(Directory d, Analyzer a, boolean create, MaxFieldLength mfl) throws CorruptIndexException,
			LockObtainFailedException, IOException {
		super(d, a, create, mfl);
	}

	public SuperIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl,
			IndexCommit commit) throws CorruptIndexException, LockObtainFailedException, IOException {
		super(d, a, deletionPolicy, mfl, commit);
	}

	public SuperIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl)
			throws CorruptIndexException, LockObtainFailedException, IOException {
		super(d, a, deletionPolicy, mfl);
	}

	public SuperIndexWriter(Directory d, Analyzer a, MaxFieldLength mfl) throws CorruptIndexException,
			LockObtainFailedException, IOException {
		super(d, a, mfl);
	}
	
	public int numSuperDocs() {
		return -1;
	}
	
	private boolean specialIndexing = false;
	
	public SuperIndexWriter addSuperDocument(SuperDocument document, Analyzer analyzer) throws CorruptIndexException, IOException {
		if (specialIndexing) {
			Directory directory = new RAMDirectory();
			IndexWriter indexWriter = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
			boolean prime = false;
			for (Document doc : document.getAllDocumentsForIndexing()) {
				if (!prime) {
					doc.add(PRIME_DOC_FIELD);
					prime = true;
				}
				indexWriter.addDocument(doc, analyzer);
			}
			indexWriter.optimize();
			indexWriter.close();
			addIndexesNoOptimize(new Directory[]{directory});
		} else {
			boolean prime = false;
			for (Document doc : document.getAllDocumentsForIndexing()) {
				if (!prime) {
					doc.add(PRIME_DOC_FIELD);
					prime = true;
				}
				super.addDocument(doc, analyzer);
			}
		}
		return this;
	}
	
	public SuperIndexWriter updateSuperDocument(Term term, SuperDocument document, Analyzer analyzer) throws CorruptIndexException, IOException {
		deleteDocuments(term);
		addSuperDocument(document, analyzer);
		return this;
	}
	
	public SuperIndexWriter addSuperDocument(SuperDocument document) throws CorruptIndexException, IOException {
		addSuperDocument(document, getAnalyzer());
		return this;
	}

	public SuperIndexWriter updateSuperDocument(Term term, SuperDocument document) throws CorruptIndexException, IOException {
		updateSuperDocument(term, document, getAnalyzer());
		return this;
	}

	@Override
	public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {
		throw new RuntimeException("Not supported.");
	}

	@Override
	public void addDocument(Document doc) throws CorruptIndexException, IOException {
		throw new RuntimeException("Not supported.");
	}

}

