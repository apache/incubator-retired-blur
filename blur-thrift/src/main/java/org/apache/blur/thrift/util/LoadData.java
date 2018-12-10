package org.apache.blur.thrift.util;

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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

public class LoadData {

  private static final long _5_SECONDS = TimeUnit.SECONDS.toNanos(5);
  private static Random random = new Random();
  private static List<String> words = new ArrayList<String>();

  public static void main(String[] args) throws BlurException, TException, IOException {
    final int numberOfColumns = 3;
    int numberRows = 100000;
    final int numberRecordsPerRow = 3;
    final int numberOfFamilies = 3;
    final int numberOfWords = 30;
    int batch = 1;
    String connectionString = args[0];
    String table = args[1];
    Iface client = BlurClient.getClient(connectionString);
    runLoad(client, false, table, numberRows, numberRecordsPerRow, numberOfFamilies, numberOfColumns, numberOfWords,
        batch, new PrintWriter(System.out));
  }

  public static void runLoad(Iface client, boolean enqueue, String table, int numberRows, int numberRecordsPerRow,
      int numberOfFamilies, int numberOfColumns, int numberOfWords, int batch, PrintWriter out) throws IOException,
      BlurException, TException {
    loadWords();

    int countRow = 0;
    int countRecord = 0;
    final long start = System.currentTimeMillis();
    long s = start;

    List<RowMutation> mutations = new ArrayList<RowMutation>();

    long ts = System.nanoTime();
    for (int i = 0; i < numberRows; i++) {
      long now = System.nanoTime();
      if (ts + _5_SECONDS < now) {
        printPerformance(out, countRow, countRecord, start, s, i);
        countRow = 0;
        countRecord = 0;
        s = System.currentTimeMillis();
        ts = System.nanoTime();
      }

      RowMutation mutation = new RowMutation();
      mutation.setTable(table);
      String rowId = getRowId();
      mutation.setRowId(rowId);
      mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
      for (int j = 0; j < numberRecordsPerRow; j++) {
        mutation.addToRecordMutations(getRecordMutation(numberOfColumns, numberOfFamilies, numberOfWords));
        countRecord++;
      }
      if (batch == 1) {
        if (enqueue) {
          client.enqueueMutate(mutation);
        } else {
          client.mutate(mutation);
        }
      } else {
        mutations.add(mutation);
        if (mutations.size() >= batch) {
          if (enqueue) {
            client.enqueueMutateBatch(mutations);
          } else {
            client.mutateBatch(mutations);
          }
          mutations.clear();
        }
      }
      countRow++;
    }
    if (!mutations.isEmpty()) {
      if (enqueue) {
        client.enqueueMutateBatch(mutations);
      } else {
        client.mutateBatch(mutations);
      }
    }
    printPerformance(out, countRow, countRecord, start, s, numberRows);
  }

  private static void printPerformance(PrintWriter out, int countRow, int countRecord, final long start, long s, int i) {
    double totalSeconds = (System.currentTimeMillis() - start) / 1000.0;
    double seconds = (System.currentTimeMillis() - s) / 1000.0;
    double recordRate = countRecord / seconds;
    double rowRate = countRow / seconds;
    double avgRowRate = i / totalSeconds;
    out.printf("Rows indexed [%d] at Avg Rows [%f/s] Rows [%f/s] Records [%f/s]%n", i, avgRowRate, rowRate, recordRate);
    out.flush();
  }

  public static void loadWords() throws IOException {
    InputStream inputStream = LoadData.class.getResourceAsStream("words.txt");
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String word;
    while ((word = reader.readLine()) != null) {
      words.add(word.trim());
    }
    reader.close();
  }

  protected static RecordMutation getRecordMutation(int numberOfColumns, int numberOfFamilies, int numberOfWords) {
    RecordMutation recordMutation = new RecordMutation();
    recordMutation.setRecord(getRecord(numberOfColumns, numberOfFamilies, numberOfWords));
    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
    return recordMutation;
  }

  private static Record getRecord(int numberOfColumns, int numberOfFamilies, int numberOfWords) {
    Record record = new Record();
    record.setRecordId(getRowId());
    record.setFamily(getFamily(numberOfFamilies));
    for (int i = 0; i < numberOfColumns; i++) {
      record.addToColumns(new Column("col" + i, getWords(numberOfWords)));
    }
    return record;
  }

  private static String getWords(int numberOfWords) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numberOfWords; i++) {
      if (i != 0) {
        builder.append(' ');
      }
      builder.append(getWord());
    }
    return builder.toString();
  }

  private static String getFamily(int numberOfFamilies) {
    return "fam" + random.nextInt(numberOfFamilies);
  }

  public static String getWord() {
    return makeUpperCaseRandomly(words.get(random.nextInt(words.size())), random);
  }

  private static String makeUpperCaseRandomly(String s, Random r) {
    if (r.nextBoolean()) {
      return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
    return s;
  }

  protected static String getRowId() {
    return Long.toString(Math.abs(random.nextLong())) + "-" + Long.toString(Math.abs(random.nextLong()));
  }

}
