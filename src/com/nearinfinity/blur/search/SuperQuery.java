package com.nearinfinity.blur.search;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;

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
		return new SuperQuery((Query) query.clone());
	}

	public Query combine(Query[] queries) {
		return query.combine(queries);
	}

	public Weight createWeight(Searcher searcher) throws IOException {
		return new SuperWeight(query.createWeight(searcher));
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
		return query.weight(searcher);
	}

	public class SuperWeight extends Weight {
		
		private static final long serialVersionUID = -4832849792097064960L;
		
		private Weight weight;

		public SuperWeight(Weight weight) {
			this.weight = weight;
		}

		@Override
		public Explanation explain(IndexReader reader, int doc) throws IOException {
			throw new RuntimeException("not supported");
		}

		@Override
		public Query getQuery() {
			return SuperQuery.this;
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
			return new SuperScorer(scorer);
		}

		@Override
		public float sumOfSquaredWeights() throws IOException {
			return weight.sumOfSquaredWeights();
		}
	}
	
	public class SuperScorer extends Scorer {

		private Scorer scorer;

		protected SuperScorer(Scorer scorer) {
			super(scorer.getSimilarity());
			this.scorer = scorer;
		}

		@Override
		public float score() throws IOException {
			return scorer.score();
		}

		@Override
		public int advance(int target) throws IOException {
			return scorer.advance(target);
		}

		@Override
		public int docID() {
			return scorer.docID();
		}

		@Override
		public int nextDoc() throws IOException {
			return scorer.nextDoc();
		}
		
	}
}
