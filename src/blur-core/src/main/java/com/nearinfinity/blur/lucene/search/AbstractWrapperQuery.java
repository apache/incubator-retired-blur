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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;

@SuppressWarnings("deprecation")
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

}
