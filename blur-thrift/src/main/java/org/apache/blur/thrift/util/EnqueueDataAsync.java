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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.async.AsyncMethodCallback;
import org.apache.blur.thrift.AsyncClientPool;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.AsyncClient.enqueueMutate_call;
import org.apache.blur.thrift.generated.Blur.AsyncIface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

public class EnqueueDataAsync {

  private static Random random = new Random();
  private static List<String> words = new ArrayList<String>();

  public static void main(String[] args) throws BlurException, TException, IOException {
    loadWords();
    final int numberOfColumns = 3;
    int numberRows = 100000;
    final int numberRecordsPerRow = 2;
    final int numberOfFamilies = 3;
    final int numberOfWords = 30;
    int count = 0;
    int max = 1000;
    long start = System.currentTimeMillis();
    final String table = "test";
    AsyncClientPool pool = new AsyncClientPool();
    AsyncIface client = pool.getClient(Blur.AsyncIface.class, args[0]);
    for (int i = 0; i < numberRows; i++) {
      if (count >= max) {
        double seconds = (System.currentTimeMillis() - start) / 1000.0;
        double rate = i / seconds;
        System.out.println("Rows indexed [" + i + "] at [" + rate + "/s]");
        count = 0;
      }
      client.enqueueMutate(
          getRowMutation(table, numberRecordsPerRow, numberOfColumns, numberOfFamilies, numberOfWords),
          new AsyncMethodCallback<Blur.AsyncClient.enqueueMutate_call>() {
            @Override
            public void onError(Exception exception) {

            }

            @Override
            public void onComplete(enqueueMutate_call response) {

            }
          });
      count++;
    }
  }

  private static RowMutation getRowMutation(String table, int numberRecordsPerRow, int numberOfColumns,
      int numberOfFamilies, int numberOfWords) {
    RowMutation mutation = new RowMutation();
    mutation.setTable(table);
    String rowId = getRowId();
    mutation.setRowId(rowId);
    mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    for (int j = 0; j < numberRecordsPerRow; j++) {
      mutation.addToRecordMutations(getRecordMutation(numberOfColumns, numberOfFamilies, numberOfWords));
    }
    return mutation;
  }

  private static void loadWords() throws IOException {
    InputStream inputStream = EnqueueDataAsync.class.getResourceAsStream("words.txt");
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    try {
      String word;
      while ((word = reader.readLine()) != null) {
        words.add(word.trim());
      }
    } finally {
      reader.close();
    }
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

  private static String getWord() {
    return words.get(random.nextInt(words.size()));
  }

  protected static String getRowId() {
    return Long.toString(Math.abs(random.nextLong())) + "-" + Long.toString(Math.abs(random.nextLong()));
  }

}
