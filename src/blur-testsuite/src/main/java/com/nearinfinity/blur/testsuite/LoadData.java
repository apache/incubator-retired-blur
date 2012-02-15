package com.nearinfinity.blur.testsuite;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.RecordMutationType;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;

public class LoadData {

  private static Random random = new Random();
  private static List<String> words = new ArrayList<String>();

  public static void main(String[] args) throws BlurException, TException, IOException {
    loadWords();
    final boolean wal = false;
    final int numberOfColumns = 3;
    int numberRows = 100000;
    final int numberRecordsPerRow = 2;
    final int numberOfFamilies = 3;
    final int numberOfWords = 30;
    int count = 0;
    int max = 100;
    long start = System.currentTimeMillis();
    final String table = "test-table";
    for (int i = 0; i < numberRows; i++) {
      if (count >= max) {
        double seconds = (System.currentTimeMillis() - start) / 1000.0;
        double rate = i / seconds;
        System.out.println("Rows indexed [" + i + "] at [" + rate + "/s]");
        count = 0;
      }
      BlurClientManager.execute(args[0], new BlurCommand<String>() {
        @Override
        public String call(Client client) throws BlurException, TException {
          RowMutation mutation = new RowMutation();
          mutation.setTable(table);
          String rowId = getRowId();
          mutation.setRowId(rowId);
          mutation.setWal(wal);
          mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
          for (int j = 0; j < numberRecordsPerRow; j++) {
            mutation.addToRecordMutations(getRecordMutation(numberOfColumns, numberOfFamilies, numberOfWords));
          }
          client.mutate(mutation);
          return rowId;
        }
      }, 2, 100, 100);
      count++;
    }
  }

  private static void loadWords() throws IOException {
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

  private static String getWord() {
    return words.get(random.nextInt(words.size()));
  }

  protected static String getRowId() {
    return Long.toString(Math.abs(random.nextLong())) + "-" + Long.toString(Math.abs(random.nextLong()));
  }

}
