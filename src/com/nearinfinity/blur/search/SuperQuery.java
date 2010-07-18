package com.nearinfinity.blur.search;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;

import com.nearinfinity.blur.utils.BlurBitSet;
import com.nearinfinity.blur.utils.PrimeDocCache;

public class SuperQuery extends Query {

	private static final long serialVersionUID = -5901574044714034398L;
	private Query query;
	private boolean rewritten;
	
	public SuperQuery(Query query) {
		this(query,false);
	}
	
	public SuperQuery(Query query, boolean rewritten) {
		this.query = query;
		this.rewritten = rewritten;
	}

	public Object clone() {
		return new SuperQuery((Query) query.clone(),rewritten);
	}

	public Query combine(Query[] queries) {
		return query.combine(queries);
	}

	public Weight createWeight(Searcher searcher) throws IOException {
		return new SuperWeight(query.createWeight(searcher),query.toString(),this);
	}

	public boolean equals(Object obj) {
		return query.equals(obj);
	}

	public void extractTerms(Set<Term> terms) {
		query.extractTerms(terms);
	}

	public float getBoost() {
		return query.getBoost();
	}

	public Similarity getSimilarity(Searcher searcher) {
		return query.getSimilarity(searcher);
	}

	public int hashCode() {
		return query.hashCode();
	}

	public Query rewrite(IndexReader reader) throws IOException {
		if (rewritten) {
			return this;
		}
		return new SuperQuery(query.rewrite(reader),true);
	}

	public void setBoost(float b) {
		query.setBoost(b);
	}

	public String toString() {
		return "super:{" + query.toString() + "}";
	}

	public String toString(String field) {
		return "super:{" + query.toString(field) + "}";
	}

	public Weight weight(Searcher searcher) throws IOException {
		return createWeight(searcher);
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
		
		private static final Log LOG = LogFactory.getLog(SuperScorer.class);
		private Scorer scorer;
		private float superDocScore = 1;
		private BlurBitSet bitSet;
		private int nextPrimeDoc;
		private int primeDoc;
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
				if (!isScorerExhausted(nextPrimeDoc)) {
					primeDoc = nextPrimeDoc;
					nextPrimeDoc = NO_MORE_DOCS;
					return primeDoc;
				}
				return primeDoc = doc;
			}
			if (target > doc || doc == -1) {
				doc = scorer.advance(target);
			}
			if (isScorerExhausted(doc)) {
				return primeDoc;
			}
			return gatherAllHitsSuperDoc(doc);
		}

		@Override
		public int nextDoc() throws IOException {
			int doc = scorer.docID();
			if (isScorerExhausted(doc)) {
				if (!isScorerExhausted(nextPrimeDoc)) {
					primeDoc = nextPrimeDoc;
					nextPrimeDoc = NO_MORE_DOCS;
					return primeDoc;
				}
				return primeDoc = doc;
			}
			if (doc == -1) {
				doc = scorer.nextDoc();
			}
			if (isScorerExhausted(doc)) {
				return primeDoc;
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
//			int prevSetBit = bitSet.prevSetBit(doc);
//			if (prevSetBit < 0) {
//				throw new RuntimeException("Possible Corrupt Index");
//			}
//			return prevSetBit;
		}

		private boolean isScorerExhausted(int doc) {
			return doc == NO_MORE_DOCS ? true : false;
		}
	}
}
