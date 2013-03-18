package org.apache.blur.lucene.search;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class SuperParser extends QueryParser {

  private static final String MUST_NOT_STRING = "-";
  private static final String MUST_STRING = "+";
  private static final Pattern PATTERN = Pattern.compile("([-+]{0,1})\\s*?super\\s*?\\:\\s*?\\<(.*?)\\>");
  private static final Pattern CHECK = Pattern.compile("super\\s*?\\:\\s*?\\<");

  private final Analyzer a;
  private final String f;
  private final Version matchVersion;
  private final Term defaultPrimeDocTerm;
  private final ScoreType defaultScoreType;

  public SuperParser(Version matchVersion, String f, Analyzer a, ScoreType defaultScoreType, Term defaultPrimeDocTerm) {
    super(matchVersion, f, a);
    this.matchVersion = matchVersion;
    this.f = f;
    this.a = a;
    this.defaultPrimeDocTerm = defaultPrimeDocTerm;
    this.defaultScoreType = defaultScoreType;
  }

  @Override
  public Query parse(String query) throws ParseException {
    Matcher matcher = PATTERN.matcher(query);
    BooleanQuery booleanQuery = null;
    while (matcher.find()) {
      int count = matcher.groupCount();
      for (int i = 0; i < count; i++) {
        String occurString = matcher.group(i + 1);
        i++;
        String superQueryStr = matcher.group(i + 1);
        Matcher matcherCheck = CHECK.matcher(superQueryStr);
        if (matcherCheck.find()) {
          throw new ParseException("Embedded super queries are not allowed [" + query + "].");
        }

        if (booleanQuery == null) {
          booleanQuery = new BooleanQuery();
        }

        Occur occur = getOccur(occurString);
        QueryParser parser = new QueryParser(matchVersion, f, a);

        Query superQuery = parser.parse(superQueryStr);
        booleanQuery.add(new SuperQuery(superQuery, defaultScoreType, defaultPrimeDocTerm), occur);
      }
    }
    if (booleanQuery == null) {
      return super.parse(query);
    }
    return booleanQuery;
  }

  private Occur getOccur(String occurString) {
    if (occurString.equals(MUST_STRING)) {
      return Occur.MUST;
    }
    if (occurString.equals(MUST_NOT_STRING)) {
      return Occur.MUST_NOT;
    }
    return Occur.SHOULD;
  }

}
