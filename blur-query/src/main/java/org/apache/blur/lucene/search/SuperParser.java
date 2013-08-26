package org.apache.blur.lucene.search;

import java.util.HashMap;
import java.util.Map;

import org.apache.blur.analysis.FieldManager;
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

  interface Group {
    void match(int start, int end) throws ParseException;
  }

  private static final String SEP = ".";
  private final boolean _superSearch;
  private final Filter _queryFilter;
  private final ScoreType _scoreType;
  private final Version _matchVersion;
  private final Term _defaultPrimeDocTerm;
  private final String _prefixToSub = "______SUPERBASEFIELD_";
  private int _lastStart = 0;
  private int _lastEnd = 0;

  public SuperParser(Version matchVersion, FieldManager fieldManager, boolean superSearch, Filter queryFilter,
      ScoreType scoreType, Term defaultPrimeDocTerm) {
    super(matchVersion, SUPER, null, fieldManager);
    _matchVersion = matchVersion;
    _superSearch = superSearch;
    _queryFilter = queryFilter;
    _scoreType = scoreType;
    _defaultPrimeDocTerm = defaultPrimeDocTerm;
    setAutoGeneratePhraseQueries(true);
    setAllowLeadingWildcard(true);
  }

  public Query parse(final String queryStr) throws ParseException {
    final Map<String, Query> subQueries = new HashMap<String, Query>();
    final StringBuilder builder = new StringBuilder();
    match(queryStr, new Group() {
      int _subQueryIndex = 0;

      @Override
      public void match(int start, int end) throws ParseException {
        if (_lastStart != start) {
          builder.append(queryStr.substring(_lastEnd, start));
        }
        String realQuery = queryStr.substring(start, end);
        LOG.debug("Realquery [{0}]", realQuery);
        String superQueryStr = getMatchText(realQuery);
        LOG.debug("Parseable sub query [{0}]", superQueryStr);
        String key = _prefixToSub + _subQueryIndex;
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
        builder.append(_prefixToSub).append(':').append(_subQueryIndex);
        _subQueryIndex++;
        _lastStart = start;
        _lastEnd = end;
      }

      private String getMatchText(String match) {
        return match.substring(1, match.length() - 1);
      }
    });
    if (_lastEnd < queryStr.length()) {
      builder.append(queryStr.substring(_lastEnd));
    }
    Query query = super.parse(builder.toString());
    return reprocess(replaceRealQueries(query, subQueries));
  }

  private static void match(String source, Group group) throws ParseException {
    int line = 1;
    int length = source.length();
    int start = -1;
    char p = 0;
    int column = 1;
    for (int i = 0; i < length; i++) {
      char c = source.charAt(i);
      if (c == '<') {
        if (p != '\\') {
          if (start == -1) {
            start = i;
          } else {
            throw new ParseException("Cannot parse '" + source + "': Encountered \"<\" at line " + line + ", column "
                + column + ".");
          }
        }
      } else if (c == '>') {
        if (p != '\\') {
          if (start != -1) {
            group.match(start, i + 1);
            start = -1;
          } else {
            throw new ParseException("Cannot parse '" + source + "': Encountered \">\" at line " + line + ", column "
                + column + ".");
          }
        }
      } else if (c == '\n') {
        line++;
        column = 0;
      }
      column++;
      p = c;
    }
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
    return new BlurQueryParser(_matchVersion, SUPER, _fieldNames,_fieldManager);
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
