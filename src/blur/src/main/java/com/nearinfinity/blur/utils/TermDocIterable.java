package com.nearinfinity.blur.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;

public class TermDocIterable implements Iterable<Document> {

	private TermDocs termDocs;
	private IndexReader reader;
	
	public TermDocIterable(TermDocs termDocs, IndexReader reader) {
		this.termDocs = termDocs;
		this.reader = reader;
	}

	@Override
	public Iterator<Document> iterator() {
		return new Iterator<Document>() {
			private boolean hasNext = getNext();

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public Document next() {
				Document doc;
				try {
					doc = getDoc();
					hasNext = getNext();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return doc;
			}

			@Override
			public void remove() {

			}
		};
	}
	
	private Document getDoc() throws IOException {
		return reader.document(termDocs.doc());
	}

	private boolean getNext() {
		try {
			boolean next = termDocs.next();
			while (reader.isDeleted(termDocs.doc())) {
				next = termDocs.next();
			}
			if (!next) {
				termDocs.close();
			}
			return next;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
