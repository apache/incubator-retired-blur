package com.nearinfinity.blur.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Weight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.utils.PrimeDocCache;
import com.nearinfinity.blur.utils.bitset.BlurBitSet;

public class SuperQuery extends AbstractWrapperQuery {

	private static final long serialVersionUID = -5901574044714034398L;
	
	public SuperQuery(Query query) {
		super(query,false);
	}
	
	public SuperQuery(Query query, boolean rewritten) {
		super(query,rewritten);
	}

	public Object clone() {
		return new SuperQuery((Query) query.clone(),rewritten);
	}

	public Weight createWeight(Searcher searcher) throws IOException {
		return new SuperWeight(query.createWeight(searcher),query.toString(),this);
	}

	public Query rewrite(IndexReader reader) throws IOException {
		if (rewritten) {
			return this;
		}
		return new SuperQuery(query.rewrite(reader),true);
	}

	public String toString() {
		return "super:{" + query.toString() + "}";
	}

	public String toString(String field) {
		return "super:{" + query.toString(field) + "}";
	}

	public static class SuperWeight extends Weight {
		
		private static final long serialVersionUID = -4832849792097064960L;
		
		private Weight weight;
		private String originalQueryStr;
		private Query query;

		public SuperWeight(Weight weight, String originalQueryStr, Query query) {
			this.weight = weight;
			this.originalQueryStr = originalQueryStr;
			this.query = query;
		}

		@Override
		public Explanation explain(IndexReader reader, int doc) throws IOException {
			throw new RuntimeException("not supported");
		}

		@Override
		public Query getQuery() {
			return query;
		}

		@Override
		public float getValue() {
			return weight.getValue();
		}

		@Override
		public void normalize(float norm) {
			weight.normalize(norm);
		}

		@Override
		public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
			Scorer scorer = weight.scorer(reader, scoreDocsInOrder, topScorer);
			return new SuperScorer(scorer,PrimeDocCache.getPrimeDoc(reader),originalQueryStr);
		}

		@Override
		public float sumOfSquaredWeights() throws IOException {
			return weight.sumOfSquaredWeights();
		}
	}
	
	@SuppressWarnings("unused")
	public static class SuperScorer extends Scorer {
		
		private final static Logger LOG = LoggerFactory.getLogger(SuperScorer.class);
		private Scorer scorer;
		private float superDocScore = 1;
		private BlurBitSet bitSet;
		private int nextPrimeDoc;
		private int primeDoc = -1;
		private String originalQueryStr;

		protected SuperScorer(Scorer scorer, BlurBitSet bitSet, String originalQueryStr) {
			super(scorer.getSimilarity());
			this.scorer = scorer;
			this.bitSet = bitSet;
			this.originalQueryStr = originalQueryStr;
		}

		@Override
		public float score() throws IOException {
			return superDocScore;
		}
		
		@Override
		public int docID() {
			return primeDoc;
		}

		@Override
		public int advance(int target) throws IOException {
			int doc = scorer.docID();
			if (isScorerExhausted(doc)) {
				return primeDoc = doc;
			}
			if (target > doc || doc == -1) {
				doc = scorer.advance(target);
			}
			if (isScorerExhausted(doc)) {
				return primeDoc == -1 ? primeDoc = doc : primeDoc;
			}
			return gatherAllHitsSuperDoc(doc);
		}

		@Override
		public int nextDoc() throws IOException {
			int doc = scorer.docID();
			if (isScorerExhausted(doc)) {
				return primeDoc = doc;
			}
			if (doc == -1) {
				doc = scorer.nextDoc();
			}
			if (isScorerExhausted(doc)) {
				return primeDoc == -1 ? primeDoc = doc : primeDoc;
			}
			return gatherAllHitsSuperDoc(doc);
		}

		private int gatherAllHitsSuperDoc(int doc) throws IOException {
			reset();
			primeDoc = getPrimeDoc(doc);
			nextPrimeDoc = getNextPrimeDoc(doc);
			superDocScore += scorer.score();
			while ((doc = scorer.nextDoc()) < nextPrimeDoc) {
				superDocScore += scorer.score();
			}
			return primeDoc;
		}
		
		private void reset() {
			superDocScore = 0;
		}

		private int getNextPrimeDoc(int doc) {
			int nextSetBit = bitSet.nextSetBit(doc+1);
			return nextSetBit == -1 ? NO_MORE_DOCS : nextSetBit;
		}

		private int getPrimeDoc(int doc) {
			while (!bitSet.get(doc)) {
				doc--;
				if (doc <= 0) {
					return 0;
				}
			}
			return doc;
		}

		private boolean isScorerExhausted(int doc) {
			return doc == NO_MORE_DOCS ? true : false;
		}
	}
}
