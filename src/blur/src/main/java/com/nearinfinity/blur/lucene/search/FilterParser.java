package com.nearinfinity.blur.lucene.search;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.manager.FilterManager;
import com.nearinfinity.blur.thrift.BlurShardServer;

public class FilterParser extends QueryParser {
	
	private static final Logger LOG = LoggerFactory.getLogger(BlurShardServer.class);
	private static final String FILTERED_QUERY = "FILTERED_QUERY";
	private String table;
	private FilterManager filterManager;

	public FilterParser(String table, FilterManager filterManager) {
		super(Version.LUCENE_30, FILTERED_QUERY, new WhitespaceAnalyzer());
		this.table = table;
		this.filterManager = filterManager;
	}

	public Filter parseFilter(String filter) throws ParseException {
		return new QueryWrapperFilter(parse(filter));
	}

	@Override
	protected Query newTermQuery(Term term) {
		if (FILTERED_QUERY.equals(term.field())) {
			Filter filter = filterManager.getFilter(table, term.text());
			if (filter == null) {
				LOG.debug("Dynamic term [" + term +
						"] not found for table [" + table +
						"].");
				return super.newTermQuery(term);
			}
			return new ConstantScoreQuery(filter);
		} else {
			return super.newTermQuery(term);
		}
	}

	@Override
	protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
		throw new RuntimeException("Fuzzy queries are not allowed in filter.");
	}

	@Override
	protected Query newMatchAllDocsQuery() {
		throw new RuntimeException("Match all docs queries are not allowed in filter.");
	}

	@Override
	protected MultiPhraseQuery newMultiPhraseQuery() {
		throw new RuntimeException("Multi phrase queries are not allowed in filter.");
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		throw new RuntimeException("Phrase queries are not allowed in filter.");
	}

	@Override
	protected Query newPrefixQuery(Term prefix) {
		throw new RuntimeException("Prefix queries are not allowed in filter.");
	}

	@Override
	protected Query newRangeQuery(String field, String part1, String part2, boolean inclusive) {
		throw new RuntimeException("Range queries are not allowed in filter.");
	}

	@Override
	protected Query newWildcardQuery(org.apache.lucene.index.Term t) {
		throw new RuntimeException("Wild card queries are not allowed in filter.");
	}

}
