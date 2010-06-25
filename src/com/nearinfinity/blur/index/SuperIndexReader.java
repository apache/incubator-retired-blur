package com.nearinfinity.blur.index;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.OpenBitSet;


public class SuperIndexReader extends IndexReader {
	
	private static final Term PRIME_DOC_TERM = new Term(SuperDocument.PRIME_DOC,SuperDocument.PRIME_DOC_VALUE);
	private IndexReader indexReader;
	private OpenBitSet[] primeDocBitSets;

	public SuperIndexReader(IndexReader indexReader) throws IOException {
		this.indexReader = indexReader;
		IndexReader[] sequentialSubReaders = indexReader.getSequentialSubReaders();
		primeDocBitSets = new OpenBitSet[sequentialSubReaders.length];
		for (int i = 0; i < sequentialSubReaders.length; i++) {
			primeDocBitSets[i] = new OpenBitSet(sequentialSubReaders[i].maxDoc());
		}
		populatePrimeDocBitSets(primeDocBitSets,sequentialSubReaders);
	}
	
	public SuperIndexReader(Directory directory) throws CorruptIndexException, IOException {
		this(IndexReader.open(directory));
	}

	private static void populatePrimeDocBitSets(OpenBitSet[] primeDocBS, IndexReader[] readers) throws IOException {
		for (int i = 0; i < readers.length; i++) {
			populatePrimeDocBitSet(primeDocBS[i],readers[i]);
		}
	}

	private static void populatePrimeDocBitSet(OpenBitSet primeDocBS, IndexReader reader) throws IOException {
		TermDocs termDocs = reader.termDocs(PRIME_DOC_TERM);
		while (termDocs.next()) {
			primeDocBS.set(termDocs.doc());
		}
	}

	public int numSuperDocs() {
		return -1;
	}
	
	@Override
	public int docFreq(Term t) throws IOException {
		return indexReader.docFreq(t);
	}

	@Override
	public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
		return indexReader.document(n, fieldSelector);
	}

	@Override
	public Collection<String> getFieldNames(FieldOption fldOption) {
		return indexReader.getFieldNames(fldOption);
	}

	@Override
	public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
		return indexReader.getTermFreqVector(docNumber, field);
	}

	@Override
	public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
		indexReader.getTermFreqVector(docNumber, mapper);
	}

	@Override
	public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
		indexReader.getTermFreqVector(docNumber, field, mapper);
	}

	@Override
	public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
		return indexReader.getTermFreqVectors(docNumber);
	}

	@Override
	public boolean hasDeletions() {
		return indexReader.hasDeletions();
	}

	@Override
	public boolean isDeleted(int n) {
		return indexReader.isDeleted(n);
	}

	@Override
	public int maxDoc() {
		return indexReader.maxDoc();
	}

	@Override
	public byte[] norms(String field) throws IOException {
		return indexReader.norms(field);
	}

	@Override
	public void norms(String field, byte[] bytes, int offset) throws IOException {
		indexReader.norms(field, bytes, offset);
	}

	@Override
	public int numDocs() {
		return indexReader.numDocs();
	}

	@Override
	public TermDocs termDocs() throws IOException {
		return indexReader.termDocs();
	}

	@Override
	public TermPositions termPositions() throws IOException {
		return indexReader.termPositions();
	}

	@Override
	public TermEnum terms() throws IOException {
		return indexReader.terms();
	}

	@Override
	public TermEnum terms(Term t) throws IOException {
		return indexReader.terms(t);
	}
	
	@Override
	protected void doClose() throws IOException {
		indexReader.close();
	}

	@Override
	protected void doCommit(Map<String, String> commitUserData) throws IOException {
		indexReader.commit(commitUserData);
	}

	@Override
	protected void doDelete(int docNum) throws CorruptIndexException, IOException {
		indexReader.deleteDocument(docNum);
	}

	@Override
	protected void doSetNorm(int doc, String field, byte value) throws CorruptIndexException, IOException {
		indexReader.setNorm(doc, field, value);
	}

	@Override
	protected void doUndeleteAll() throws CorruptIndexException, IOException {
		indexReader.undeleteAll();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		indexReader.close();
	}
	
}
