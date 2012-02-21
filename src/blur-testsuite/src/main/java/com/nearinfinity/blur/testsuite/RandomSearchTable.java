package com.nearinfinity.blur.testsuite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public class RandomSearchTable {

  public static void main(String[] args) throws BlurException, TException, IOException {
    String connectionStr = args[0];
    final String tableName = args[1];
    int numberOfTerms = Integer.parseInt(args[2]);
    int numberOfSearchesPerPass = Integer.parseInt(args[3]);
    int numberOfTermsPerQuery = Integer.parseInt(args[4]);
    List<String> sampleOfTerms = getSampleOfTerms(connectionStr,tableName,numberOfTerms);
//    for (String term : sampleOfTerms) {
//      System.out.println(term);
//    }
    runSearches(connectionStr,tableName,sampleOfTerms,numberOfSearchesPerPass,numberOfTermsPerQuery);
  }

  private static void runSearches(String connectionStr, final String tableName, List<String> sampleOfTerms, int numberOfSearchesPerPass, int numberOfTermsPerQuery) throws BlurException, TException, IOException {
    Random random = new Random();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numberOfSearchesPerPass; i++) {
      builder.setLength(0);
      String query = generateQuery(builder,random,sampleOfTerms,numberOfTermsPerQuery);
      System.out.println(query);
      final BlurQuery blurQuery = new BlurQuery();
      blurQuery.simpleQuery = new SimpleQuery();
      blurQuery.simpleQuery.queryStr = query;
      blurQuery.allowStaleData = false;
      BlurResults results = BlurClientManager.execute(connectionStr, new BlurCommand<BlurResults>() {
        @Override
        public BlurResults call(Client client) throws BlurException, TException {
          return client.query(tableName, blurQuery);
        }
      });
      System.out.println(results.totalResults);
    }
  }

  private static String generateQuery(StringBuilder builder, Random random, List<String> sampleOfTerms, int numberOfTermsPerQuery) {
    for (int i = 0; i < numberOfTermsPerQuery; i++) {
      builder.append(getRandomTerm(sampleOfTerms,random)).append(' ');
    }
    return builder.toString().trim();
  }

  private static String getRandomTerm(List<String> sampleOfTerms, Random random) {
    int index = random.nextInt(sampleOfTerms.size());
    return sampleOfTerms.get(index);
  }

  private static List<String> getSampleOfTerms(String connectionStr, String tableName, int numberOfTerms) throws BlurException, TException, IOException {
    List<String> sampleOfTerms = new ArrayList<String>();
    Set<String> fields = getFields(connectionStr, tableName);
    for (String field : fields) {
      Set<String> randomSampleOfTerms = getRandomSampleOfTerms(connectionStr, tableName, field, numberOfTerms);
      for (String term : randomSampleOfTerms) {
        sampleOfTerms.add(field + ":" + term);
      }
    }
    Collections.shuffle(sampleOfTerms);
    return sampleOfTerms;
  }

  private static Set<String> getRandomSampleOfTerms(String connectionStr, final String tableName, final String field, final int numberOfTerms) throws BlurException, TException, IOException {
    return BlurClientManager.execute(connectionStr, new BlurCommand<Set<String>>() {
      @Override
      public Set<String> call(Client client) throws BlurException, TException {
        String[] split = field.split("\\.");
        String columnFamily = split[0];
        String columnName = split[1];
        List<String> terms = client.terms(tableName, columnFamily, columnName, "", (short) numberOfTerms);
        return new HashSet<String>(terms);
      }
    });
  }

  private static Set<String> getFields(String connectionStr, final String tableName) throws BlurException, TException, IOException {
    return BlurClientManager.execute(connectionStr, new BlurCommand<Set<String>>() {
      @Override
      public Set<String> call(Client client) throws BlurException, TException {
        Schema schema = client.schema(tableName);
        Set<String> fields = new HashSet<String>();
        for (String cf : schema.columnFamilies.keySet()) {
          for (String field : schema.columnFamilies.get(cf)) {
            fields.add(cf + "." + field);
          }
        }
        return fields;
      }
    });
  }

}
