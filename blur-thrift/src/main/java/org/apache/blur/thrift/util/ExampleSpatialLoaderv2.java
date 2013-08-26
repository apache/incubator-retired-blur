package org.apache.blur.thrift.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

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

public class ExampleSpatialLoaderv2 {

  public static void main(String[] args) throws IOException, BlurException, TException {
    Iface client = BlurClient.getClient(args[0]);
    String tableName = args[1];
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(
        "/Users/amccurry/Downloads/zipcode/zipcode.csv")));
    String line;
    List<String> header = new ArrayList<String>();
    List<RowMutation> batch = new ArrayList<RowMutation>();
    int count = 0;
    int total = 0;
    int max = 1000;
    while ((line = reader.readLine()) != null) {
      if (count >= max) {
        System.out.println(total);
        count = 0;
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] split = line.split("\",\"");
      if (header.isEmpty()) {
        for (String s : split) {
          header.add(trim(s));
        }
      } else {
        Record record = new Record();
        String zip = trim(split[0]);
        record.setRecordId(zip);
        record.setFamily("zip");
        record.addToColumns(new Column("zip", zip));
        record.addToColumns(new Column("city", trim(split[1])));
        record.addToColumns(new Column("state", trim(split[2])));
        record.addToColumns(new Column("location", trim(split[3]) + "," + trim(split[4])));
        record.addToColumns(new Column("timezone", trim(split[5])));
        record.addToColumns(new Column("dst", trim(split[6])));

        RowMutation mutation = new RowMutation();
        mutation.setTable(tableName);
        mutation.setRowId(zip);
        mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
        List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
        RecordMutation recordMutation = new RecordMutation();
        recordMutation.setRecord(record);
        recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
        recordMutations.add(recordMutation);
        mutation.setRecordMutations(recordMutations);

        // zip=99950
        // city=Ketchikan
        // state=AK
        // latitude=55.875767
        // longitude=-131.46633
        // timezone=-9
        // dst=1
        batch.add(mutation);
        count++;
        total++;
        if (batch.size() > 100) {
          client.mutateBatch(batch);
          batch.clear();
        }
      }
    }
    if (batch.size() > 0) {
      client.mutateBatch(batch);
    }
  }

  private static String trim(String s) {
    return s.replace("\"", "");
  }

}
