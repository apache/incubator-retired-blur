package com.nearinfinity.blur.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.CharStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParserTokenManager;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class SuperParser extends QueryParser {
	
	public static final String SUPER = "SUPER";
	private static final String SEP = ".";
	private Map<Query,String> fieldNames = new HashMap<Query, String>();
	
	public static void main(String[] args) throws ParseException {
		SuperParser parser = new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT));
		Query query = parser.parse("address.street:sulgrave +(person.firstname:aaron person.lastname:mccurry +(person.gender:(unknown male)))");
		System.out.println(query);
	}

	protected SuperParser(CharStream stream) {
		super(stream);
	}

	public SuperParser(QueryParserTokenManager tm) {
		super(tm);
	}

	public SuperParser(Version matchVersion, Analyzer a) {
		super(matchVersion, SUPER, a);
	}

	@Override
	public Query parse(String query) throws ParseException {
		return reprocess(super.parse(query));
	}

	@Override
	protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
		return addField(super.newFuzzyQuery(term, minimumSimilarity, prefixLength),term.field());
	}

	@Override
	protected Query newMatchAllDocsQuery() {
		throw new RuntimeException("not supported yet");
	}

	@Override
	protected MultiPhraseQuery newMultiPhraseQuery() {
		throw new RuntimeException("not supported yet");
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		throw new RuntimeException("not supported yet");
	}

	@Override
	protected Query newPrefixQuery(Term prefix) {
		return addField(super.newPrefixQuery(prefix),prefix.field());
	}

	@Override
	protected Query newRangeQuery(String field, String part1, String part2, boolean inclusive) {
		return addField(super.newRangeQuery(field, part1, part2, inclusive),field);
	}

	@Override
	protected Query newTermQuery(Term term) {
		return addField(super.newTermQuery(term),term.field());
	}

	@Override
	protected Query newWildcardQuery(Term t) {
		return addField(super.newWildcardQuery(t),t.field());
	}

	private Query reprocess(Query query) {
		if (query == null) {
			return query;
		}
		if (query instanceof BooleanQuery) {
			BooleanQuery booleanQuery = (BooleanQuery) query;
			if (isSameGroupName(booleanQuery)) {
				return new SuperQuery(booleanQuery);
			} else {
				List<BooleanClause> clauses = booleanQuery.clauses();
				for (BooleanClause clause : clauses) {
					clause.setQuery(reprocess(clause.getQuery()));
				}
				return booleanQuery;
			}
		} else {
			return new SuperQuery(query);
		}
	}

	private boolean isSameGroupName(BooleanQuery booleanQuery) {
		String groupName = findFirstGroupName(booleanQuery);
		if (groupName == null) {
			return false;
		}
		return isSameGroupName(booleanQuery,groupName);
	}
	
	private boolean isSameGroupName(Query query, String groupName) {
		if (query instanceof BooleanQuery) {
			BooleanQuery booleanQuery = (BooleanQuery) query;
			for (BooleanClause clause : booleanQuery.clauses()) {
				if (!isSameGroupName(clause.getQuery(), groupName)) {
					return false;
				}
			}
			return true;
		} else {
			String fieldName = fieldNames.get(query);
			String currentGroupName = getGroupName(fieldName);
			if (groupName.equals(currentGroupName)) {
				return true;
			}
			return false;
		}
	}

	private String getGroupName(String fieldName) {
		if (fieldName == null) {
			return null;
		}
		int index = fieldName.indexOf(SEP);
		if (index < 0) {
			return null;
		}
		return fieldName.substring(0,index);
	}

	private String findFirstGroupName(Query query) {
		if (query instanceof BooleanQuery) {
			BooleanQuery booleanQuery = (BooleanQuery) query;
			for (BooleanClause clause : booleanQuery.clauses()) {
				return findFirstGroupName(clause.getQuery());
			}
			return null;
		} else {
			return getGroupName(fieldNames.get(query));
		}
	}

	private Query addField(Query q, String field) {
		fieldNames.put(q, field);
		return q;
	}
}
