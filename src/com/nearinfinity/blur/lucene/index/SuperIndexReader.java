package com.nearinfinity.blur.lucene.index;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.nearinfinity.blur.utils.PrimeDocCache;
import com.nearinfinity.blur.utils.bitset.BlurBitSet;


public class SuperIndexReader extends IndexReader {
	
	private final static Log LOG = LogFactory.getLog(SuperIndexReader.class);
	private static final Term PRIME_DOC_TERM = new Term(SuperDocument.PRIME_DOC,SuperDocument.PRIME_DOC_VALUE);
	private IndexReader indexReader;
	private Thread warmUpThread;
	
	public SuperIndexReader(IndexReader reader) throws IOException {
		this.indexReader = reader;
		init();
	}
	
	private void init() {
		warmUpThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					IndexReader[] subReaders = indexReader.getSequentialSubReaders();
					for (IndexReader reader : subReaders) {
						if (!PrimeDocCache.isPrimeDocPopulated(reader)) {
							BlurBitSet bitSet = new BlurBitSet(reader.maxDoc());
							populatePrimeDocBitSet(bitSet, reader);
							PrimeDocCache.setPrimeDoc(reader,bitSet);
						}
					}
				} catch (Exception e) {
					LOG.error("unknown error",e);
					throw new RuntimeException(e);
				}
			}

			private void populatePrimeDocBitSet(BlurBitSet primeDocBS, IndexReader reader) throws IOException {
				TermDocs termDocs = reader.termDocs(PRIME_DOC_TERM);
				while (termDocs.next()) {
					primeDocBS.set(termDocs.doc());
				}
			}
		});
		warmUpThread.setName("SuperIndexReader-Warm-Up[" + indexReader.toString() + "]");
		warmUpThread.setDaemon(true);
		warmUpThread.start();
	}

	public SuperIndexReader(Directory directory) throws CorruptIndexException, IOException {
		this(IndexReader.open(directory));
	}

	@Override
	public IndexReader[] getSequentialSubReaders() {
		return indexReader.getSequentialSubReaders();
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

	public void waitForWarmUp() throws InterruptedException {
		warmUpThread.join();
	}

	@Override
	public boolean isCurrent() throws CorruptIndexException, IOException {
		return indexReader.isCurrent();
	}
	
}
