package org.apache.blur.lucene.search;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class SuperParser extends BlurQueryParser {

  private static Log LOG = LogFactory.getLog(SuperParser.class);

  private final String _defaultField = SUPER;
  private static final Pattern PATTERN = Pattern.compile("super\\s*?\\:\\s*?\\<(.*?)\\>");
  private static final Pattern CHECK = Pattern.compile("super\\s*?\\:\\s*?\\<");
  private static final String SEP = ".";
  private final boolean _superSearch;
  private final Filter _queryFilter;
  private final ScoreType _scoreType;
  private final Version _matchVersion;
  private final Term _defaultPrimeDocTerm;
  private final String _prefixToSub = "______SUPERBASEFIELD_";

  public SuperParser(Version matchVersion, BlurAnalyzer a, boolean superSearch, Filter queryFilter,
      ScoreType scoreType, Term defaultPrimeDocTerm) {
    super(matchVersion, SUPER, a, null);
    _matchVersion = matchVersion;
    _superSearch = superSearch;
    _queryFilter = queryFilter;
    _scoreType = scoreType;
    _defaultPrimeDocTerm = defaultPrimeDocTerm;
    setAutoGeneratePhraseQueries(true);
    setAllowLeadingWildcard(true);
  }

  public Query parse(String queryStr) throws ParseException {
    Matcher matcher = PATTERN.matcher(queryStr);
    Map<String, Query> subQueries = new HashMap<String, Query>();
    int subQueryIndex = 0;
    StringBuilder builder = new StringBuilder();
    int lastStart = 0;
    int lastEnd = 0;
    while (matcher.find()) {
      int count = matcher.groupCount();
      int start = matcher.start();
      int end = matcher.end();
      if (lastStart != start) {
        builder.append(queryStr.substring(lastEnd, start));
      }
      String realQuery = queryStr.substring(start, end);
      LOG.debug("Realquery [{0}]", realQuery);
      for (int i = 0; i < count; i++) {
        String superQueryStr = matcher.group(i + 1);
        Matcher matcherCheck = CHECK.matcher(superQueryStr);
        if (matcherCheck.find()) {
          throw new ParseException("Embedded super queries are not allowed [" + queryStr + "].");
        }
        LOG.debug("Parseable sub query [{0}]", superQueryStr);
        String key = _prefixToSub + subQueryIndex;
        QueryParser newParser = getNewParser();
        Query query = newParser.parse(superQueryStr);
        if (!isSameGroupName(query)) {
          throw new ParseException("Super query [" + superQueryStr + "] cannot reference more than one column family.");
        }
        if (_superSearch) {
          query = newSuperQuery(query);
        } else {
          query = wrapFilter(query);
        }
        subQueries.put(key, query);
        builder.append(_prefixToSub).append(':').append(subQueryIndex);
        subQueryIndex++;
      }
      lastStart = start;
      lastEnd = end;
    }
    if (lastEnd < queryStr.length()) {
      builder.append(queryStr.substring(lastEnd));
    }
    Query query = super.parse(builder.toString());
    return reprocess(replaceRealQueries(query, subQueries));
  }

  private SuperQuery newSuperQuery(Query query) {
    return new SuperQuery(wrapFilter(query), _scoreType, _defaultPrimeDocTerm);
  }

  private Query wrapFilter(Query query) {
    if (_queryFilter == null) {
      return query;
    }
    return new FilteredQuery(query, _queryFilter);
  }

  private Query replaceRealQueries(Query query, Map<String, Query> subQueries) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause clause : booleanQuery) {
        clause.setQuery(replaceRealQueries(clause.getQuery(), subQueries));
      }
      return booleanQuery;
    } else if (query instanceof TermQuery) {
      TermQuery termQuery = (TermQuery) query;
      Term term = termQuery.getTerm();
      if (term.field().equals(_prefixToSub)) {
        return subQueries.get(getKey(term));
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private String getKey(Term term) {
    return term.field() + term.text();
  }

  private QueryParser getNewParser() {
    return new BlurQueryParser(_matchVersion, _defaultField, _blurAnalyzer, _fieldNames);
  }

  private Query reprocess(Query query) {
    if (query == null || !_superSearch) {
      return wrapFilter(query);
    }
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      if (containsSuperQueries(booleanQuery)) {
        for (BooleanClause bc : booleanQuery) {
          bc.setQuery(reprocess(bc.getQuery()));
        }
      } else {
        for (BooleanClause bc : booleanQuery) {
          bc.setQuery(newSuperQuery(bc.getQuery()));
        }
      }
      return booleanQuery;
    } else if (query instanceof SuperQuery) {
      return query;
    } else {
      return newSuperQuery(query);
    }
  }

  private boolean containsSuperQueries(Query query) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause bc : booleanQuery) {
        if (containsSuperQueries(bc.getQuery())) {
          return true;
        }
      }
      return false;
    } else if (query instanceof SuperQuery) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isSameGroupName(Query query) {
    if (query instanceof BooleanQuery) {
      return isSameGroupName((BooleanQuery) query);
    }
    return true;
  }

  private boolean isSameGroupName(BooleanQuery booleanQuery) {
    String groupName = findFirstGroupName(booleanQuery);
    if (groupName == null) {
      return false;
    }
    return isSameGroupName(booleanQuery, groupName);
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
      String fieldName = _fieldNames.get(query);
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
    return fieldName.substring(0, index);
  }

  private String findFirstGroupName(Query query) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause clause : booleanQuery.clauses()) {
        return findFirstGroupName(clause.getQuery());
      }
      return null;
    } else if (query instanceof SuperQuery) {
      SuperQuery sq = (SuperQuery) query;
      return findFirstGroupName(sq.getQuery());
    } else {
      String fieldName = _fieldNames.get(query);
      return getGroupName(fieldName);
    }
  }
}
