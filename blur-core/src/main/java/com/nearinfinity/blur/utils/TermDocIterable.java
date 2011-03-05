/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;

public class TermDocIterable implements Iterable<Document> {

	private TermDocs termDocs;
	private IndexReader reader;
    private FieldSelector fieldSelector;
	
	public TermDocIterable(TermDocs termDocs, IndexReader reader, FieldSelector fieldSelector) {
		this.termDocs = termDocs;
		this.reader = reader;
		this.fieldSelector = fieldSelector;
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
		return reader.document(termDocs.doc(),fieldSelector);
	}

	private boolean getNext() {
		try {
			boolean next = termDocs.next();
			if (!next) {
                termDocs.close();
                return next;
            }
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
