package org.apache.blur.testsuite;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.SimpleQuery;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.thrift.TException;


public class RandomSearchTableContinuously {

  public static void main(String[] args) throws BlurException, TException, IOException {
    if (args.length != 6) {
      System.err
          .println(RandomSearchTableContinuously.class.getName()
              + " <host1:port1,host2:port2> <table name> <# of terms to load into memory per pass> <# of searches per pass> <# of terms per query> <time in seconds between reporting progress>");
      System.exit(1);
    }
    final String connectionStr = args[0];
    final String tableName = args[1];
    final int numberOfTerms = Integer.parseInt(args[2]);
    final int numberOfSearchesPerPass = Integer.parseInt(args[3]);
    final int numberOfTermsPerQuery = Integer.parseInt(args[4]);
    final long timeBetweenReporting = TimeUnit.SECONDS.toMillis(Integer.parseInt(args[5]));
    List<String> sampleOfTerms = getSampleOfTerms(connectionStr, tableName, numberOfTerms);
    while (true) {
      runSearches(connectionStr, tableName, sampleOfTerms, numberOfSearchesPerPass, numberOfTermsPerQuery, timeBetweenReporting);
    }
  }

  private static void runSearches(String connectionStr, final String tableName, List<String> sampleOfTerms, int numberOfSearchesPerPass, int numberOfTermsPerQuery,
      long timeBetweenReporting) throws BlurException, TException, IOException {
    Random random = new Random();
    StringBuilder builder = new StringBuilder();
    final long start = System.currentTimeMillis();
    long s = start;
    long responseTime = 0;
    int count = 0;
    long resultCount = 0;
    Iface client = BlurClient.getClient(connectionStr);
    int i;
    for (i = 0; i < numberOfSearchesPerPass; i++) {
      long now = System.currentTimeMillis();
      if (s + timeBetweenReporting < now) {
        double avgSeconds = (now - start) / 1000.0;
        double seconds = (now - s) / 1000.0;
        double avgRate = i / avgSeconds;
        double rate = count / seconds;
        double responseTimeAvg = (responseTime / (double) count) / 1000000.0;
        System.out.println(System.currentTimeMillis() + "," + i + "," + responseTimeAvg + "," + rate + "," + avgRate + "," + resultCount + "," + getCount(client, tableName));
        s = now;
        responseTime = 0;
        count = 0;
        resultCount = 0;
      }

      builder.setLength(0);
      String query = generateQuery(builder, random, sampleOfTerms, numberOfTermsPerQuery);
      final BlurQuery blurQuery = new BlurQuery();
      blurQuery.simpleQuery = new SimpleQuery();
      blurQuery.simpleQuery.queryStr = query;
      blurQuery.cacheResult = false;
      blurQuery.selector = new Selector();
      long qs = System.nanoTime();

      BlurResults results = client.query(tableName, blurQuery);
      long qe = System.nanoTime();
      resultCount += results.totalResults;
      responseTime += (qe - qs);
      count++;
    }
  }

  private static long getCount(Iface client, String tableName) throws BlurException, TException {
    BlurQuery bq = new BlurQuery();
    bq.simpleQuery = new SimpleQuery();
    bq.simpleQuery.queryStr = "*";
    bq.simpleQuery.superQueryOn = false;
    bq.cacheResult = false;
    bq.useCacheIfPresent = false;
    BlurResults results = client.query(tableName, bq);
    return results.totalResults;
  }

  private static String generateQuery(StringBuilder builder, Random random, List<String> sampleOfTerms, int numberOfTermsPerQuery) {
    for (int i = 0; i < numberOfTermsPerQuery; i++) {
      builder.append(getRandomTerm(sampleOfTerms, random)).append(' ');
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

  private static Set<String> getRandomSampleOfTerms(String connectionStr, final String tableName, final String field, final int numberOfTerms) throws BlurException, TException,
      IOException {
    Iface client = BlurClient.getClient(connectionStr);
    String[] split = field.split("\\.");
    String columnFamily = split[0];
    String columnName = split[1];
    List<String> terms = client.terms(tableName, columnFamily, columnName, "", (short) numberOfTerms);
    return new HashSet<String>(terms);
  }

  private static Set<String> getFields(String connectionStr, final String tableName) throws BlurException, TException, IOException {
    Iface client = BlurClient.getClient(connectionStr);
    Schema schema = client.schema(tableName);
    Set<String> fields = new HashSet<String>();
    for (String cf : schema.columnFamilies.keySet()) {
      for (String field : schema.columnFamilies.get(cf)) {
        fields.add(cf + "." + field);
      }
    }
    return fields;
  }

}
