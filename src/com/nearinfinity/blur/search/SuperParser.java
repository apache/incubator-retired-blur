package com.nearinfinity.blur.search;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.CharStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParserTokenManager;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class SuperParser extends QueryParser {
	
	public static final String SUPER = "SUPER";
	
	public static void main(String[] args) throws ParseException {
		SuperParser parser = new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT));
		parser.parse("address.street:sulgrave +(person.firstname:aaron person.lastname:mccurry)");
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

	private Query reprocess(Query query) {
		if (query == null) {
			return query;
		}
		if (query instanceof BooleanQuery) {
			BooleanQuery booleanQuery = (BooleanQuery) query;
			List<BooleanClause> clauses = booleanQuery.clauses();
			for (BooleanClause clause : clauses) {
				
			}
		}
		return null;
	}
	
	

}
