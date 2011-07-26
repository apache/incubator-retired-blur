package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.utils.BlurConstants;

public abstract class AbstractIndexServer implements IndexServer {
	private static final Term PRIME_DOC_TERM = new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE);

	public long getRecordCount(String table) throws IOException {
		long recordCount = 0;
		Map<String, BlurIndex> indexes = getIndexes(table);
		for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
			IndexReader indexReader = null;
			try {
				indexReader = index.getValue().getIndexReader(true);
				recordCount += indexReader.numDocs();
			} finally {
				if(indexReader != null) {
					indexReader.decRef();
				}
			}
		}
		return recordCount;
	}

	public long getRowCount(String table) throws IOException {
		long rowCount = 0;
		Map<String, BlurIndex> indexes = getIndexes(table);
		for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
			IndexReader indexReader = null;
			try {
				indexReader = index.getValue().getIndexReader(true);
				TermDocs termDocs = indexReader.termDocs(PRIME_DOC_TERM);
				while(termDocs.next()) {
					if(!indexReader.isDeleted(termDocs.doc())){
						rowCount++;
					}
				}
			} finally {
				if(indexReader != null) {
					indexReader.decRef();
				}
			}
		}
		return rowCount;
	}
}
