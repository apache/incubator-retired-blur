package com.nearinfinity.blur.testsuite;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.RecordMutationType;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;

public class LoadDataContinuously {

  private static Random random = new Random();
  private static List<String> words = new ArrayList<String>();

  public static void main(String[] args) throws BlurException, TException, IOException {
    if (!(args.length == 8 || args.length == 9)) {
      System.err.println(LoadDataContinuously.class.getName() + " <host1:port1,host2:port2> <table name> <WAL true|false> <# of columns per record> <# of records per row> <# of column families> <# of words per record> <time in seconds between reporting progress> <*optional path to word dictionary>");
      System.exit(1);
    }
    if (args.length == 9) {
      loadWords(args[8]);
    } else {
      loadWords(null);
    }
    
    final Iface client = BlurClient.getClient(args[0]);
    final String table = args[1];
    final boolean wal = Boolean.parseBoolean(args[2]);
    final int numberOfColumns = Integer.parseInt(args[3]);
    final int numberRecordsPerRow = Integer.parseInt(args[4]);
    final int numberOfFamilies = Integer.parseInt(args[5]);
    final int numberOfWords = Integer.parseInt(args[6]);
    final long timeBetweenReporting = TimeUnit.SECONDS.toMillis(Integer.parseInt(args[7]));
    final long start = System.currentTimeMillis();

    long s = start;
    long recordCountTotal = 0;
    long rowCount = 0;
    
    int batchSize = 100;
    
    List<RowMutation> batch = new ArrayList<RowMutation>();
    
    long recordCount = 0;
    while (true) {
      long now = System.currentTimeMillis();
      if (s + timeBetweenReporting < now) {
        double avgSeconds = (now - start) / 1000.0;
        double seconds = (now - s) / 1000.0;
        double avgRate = recordCountTotal / avgSeconds;
        double rate = recordCount / seconds;
        System.out.println("Records indexed [" + recordCountTotal + "] Rows indexed [" + rowCount + "] at record rate [" + rate + "/s] with record avg rate [" + avgRate + "/s]");
        s = now;
        recordCount = 0;
      }

      RowMutation mutation = new RowMutation();
      mutation.setTable(table);
      String rowId = getRowId();
      mutation.setRowId(rowId);
      mutation.setWal(wal);
      mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
      for (int j = 0; j < numberRecordsPerRow; j++) {
        mutation.addToRecordMutations(getRecordMutation(numberOfColumns, numberOfFamilies, numberOfWords));
      }
      batch.add(mutation);
      if (batch.size() >= batchSize) {
        client.mutateBatch(batch);
        batch.clear();
      }
      rowCount++;
      recordCount += numberRecordsPerRow;
      recordCountTotal += numberRecordsPerRow;
    }
  }

  private static void loadWords(String path) throws IOException {
    InputStream inputStream;
    if (path == null) {
      inputStream = LoadDataContinuously.class.getResourceAsStream("words.txt");
    } else {
      inputStream = new FileInputStream(path);
    }
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
