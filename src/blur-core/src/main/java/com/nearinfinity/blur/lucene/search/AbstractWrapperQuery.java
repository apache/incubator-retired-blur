package com.nearinfinity.blur.lucene.search;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;

public abstract class AbstractWrapperQuery extends Query {
	private static final long serialVersionUID = -4512813621542220044L;
	protected Query query;
	protected boolean rewritten;
	
	public AbstractWrapperQuery(Query query) {
		this(query,false);
	}
	
	public AbstractWrapperQuery(Query query, boolean rewritten) {
		this.query = query;
		this.rewritten = rewritten;
	}

	public abstract Object clone();

	public Query combine(Query[] queries) {
		return query.combine(queries);
	}

	public abstract Weight createWeight(Searcher searcher) throws IOException;

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

	public abstract Query rewrite(IndexReader reader) throws IOException;

	public void setBoost(float b) {
		query.setBoost(b);
	}

	public abstract String toString();

	public abstract String toString(String field);

//	public Weight weight(Searcher searcher) throws IOException {
//		return createWeight(searcher);
//	}
}
