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

package com.nearinfinity.blur.lucene.search;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;

/**
 * The {@link IterablePaging} class allows for easy paging through lucene hits.
 * @author Aaron McCurry
 */
public class IterablePaging implements Iterable<ScoreDoc> {

	private static int DEFAULT_NUMBER_OF_HITS_TO_COLLECT = 1000;
	private Searcher searcher;
	private Query query;
	private TotalHitsRef totalHitsRef = new TotalHitsRef();
	private ProgressRef progressRef = new ProgressRef();
	private int skipTo;
	private int numHitsToCollect = DEFAULT_NUMBER_OF_HITS_TO_COLLECT;
	private int gather = -1;

	public IterablePaging(Searcher searcher, Query query) throws IOException {
		this(searcher,query,DEFAULT_NUMBER_OF_HITS_TO_COLLECT,null,null);
	}
	
	public IterablePaging(Searcher searcher, Query query, int numHitsToCollect) throws IOException {
		this(searcher,query,numHitsToCollect,null,null);
	}
	
	public IterablePaging(Searcher searcher, Query query, int numHitsToCollect, TotalHitsRef totalHitsRef, ProgressRef progressRef) throws IOException {
		this.query = searcher.rewrite(query);
		this.searcher = searcher;
		this.numHitsToCollect = numHitsToCollect;
		this.totalHitsRef = totalHitsRef == null ? this.totalHitsRef : totalHitsRef;
		this.progressRef = progressRef == null ? this.progressRef : progressRef;
	}
	
	public static class TotalHitsRef {
		//This is an atomic integer because more than likely if there is
		//any status sent to the user, it will be done in another thread.
		protected AtomicInteger totalHits = new AtomicInteger(0);
		public int totalHits() {
			return totalHits.get();
		}
	}
	
	public static class ProgressRef {
		//These are atomic integers because more than likely if there is
		//any status sent to the user, it will be done in another thread. 
		protected AtomicInteger skipTo = new AtomicInteger(0);
		protected AtomicInteger currentHitPosition = new AtomicInteger(0);
		protected AtomicInteger searchesPerformed = new AtomicInteger(0);
		protected AtomicLong queryTime = new AtomicLong(0);
		
		public int skipTo() {
			return skipTo.get();
		}
		public int currentHitPosition() {
			return currentHitPosition.get();
		}
		public int searchesPerformed() {
			return searchesPerformed.get();
		}
		public long queryTime() {
			return queryTime.get();
		}
	}
	
	/**
	 * Gets the total hits of the search.
	 * @return the total hits.
	 */
	public int getTotalHits() {
		return totalHitsRef.totalHits();
	}
	
	/**
	 * Allows for gathering of the total hits of this search.
	 * @param ref {@link TotalHitsRef}.
	 * @return this.
	 */
	public IterablePaging totalHits(TotalHitsRef ref) {
		totalHitsRef = ref;
		return this;
	}
	
	/**
	 * Skips the first x number of hits.
	 * @param skipTo the number hits to skip.
	 * @return this.
	 */
	public IterablePaging skipTo(int skipTo) {
		this.skipTo = skipTo;
		return this;
	}
	
	/**
	 * Only gather up to x number of hits.
	 * @param gather the number of hits to gather.
	 * @return this.
	 */
	public IterablePaging gather(int gather) {
		this.gather = gather;
		return this;
	}
	
	/**
	 * Allows for gathering the progress of the paging.
	 * @param ref the {@link ProgressRef}.
	 * @return this.
	 */
	public IterablePaging progress(ProgressRef ref) {
		this.progressRef = ref;
		return this;
	}
	
	/**
	 * The {@link ScoreDoc} iterator.
	 */
	@Override
	public Iterator<ScoreDoc> iterator() {
		return skipHits(new PagingIterator());
	}
	
	class PagingIterator implements Iterator<ScoreDoc> {
		private PagingCollector collector;
		private ScoreDoc[] scoreDocs;
		private int counter = 0;
		private int offset = 0;
		private int endPosition = gather == -1 ? Integer.MAX_VALUE : skipTo + gather;
		
		PagingIterator() {
			search();
		}
		
		void search() {
			long s = System.currentTimeMillis();
			progressRef.searchesPerformed.incrementAndGet();
			if (collector == null) {
				collector = new PagingCollector(numHitsToCollect);
			} else {
				collector = new PagingCollector(numHitsToCollect,scoreDocs[scoreDocs.length - 1]);
			}
			try {
				searcher.search(query, collector);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			totalHitsRef.totalHits.set(collector.getTotalHits());
			scoreDocs = collector.topDocs().scoreDocs;
			long e = System.currentTimeMillis();
			progressRef.queryTime.addAndGet(e-s);
		}

		@Override
		public boolean hasNext() {
			return counter < totalHitsRef.totalHits() && counter < endPosition ? true : false;
		}

		@Override
		public ScoreDoc next() {
			if (isCurrentCollectorExhausted()) {
				search();
				offset = 0;
			}
			progressRef.currentHitPosition.set(counter);
			counter++;
			return scoreDocs[offset++];
		}

		private boolean isCurrentCollectorExhausted() {
			return offset < scoreDocs.length ? false : true;
		}

		@Override
		public void remove() {
			throw new RuntimeException("read only");
		}
	}

	private Iterator<ScoreDoc> skipHits(Iterator<ScoreDoc> iterator) {
		progressRef.skipTo.set(skipTo);
		for (int i = 0; i < skipTo && iterator.hasNext(); i++) {
			//eats the hits, and moves the iterator to the desired skip to position.
			progressRef.currentHitPosition.set(i);
			iterator.next();
		}
		return iterator;
	}
	
	public static void setDefaultNumberOfHitsToCollect(int num) {
		DEFAULT_NUMBER_OF_HITS_TO_COLLECT = num;
	}
}