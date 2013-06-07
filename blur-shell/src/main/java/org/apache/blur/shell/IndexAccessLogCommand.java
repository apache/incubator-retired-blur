/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.blur.shell;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

import com.google.common.collect.Lists;

public class IndexAccessLogCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args)
      throws CommandException, TException, BlurException {
    if (args.length < 6) {
      throw new CommandException("Invalid args: " + help());
    }
    File logfile = new File(args[1]);
    int batchSize = Integer.parseInt(args[2]);
    String tablename = args[3];
    String regex = args[4];

    if (Main.debug) {
      out.println(regex);
    }

    Pattern p = Pattern.compile(regex);
    
    try {
      LineNumberReader reader = new LineNumberReader(new FileReader(logfile));
      try {
        String line;
        List<RowMutation> mutations = Lists.newArrayList();
        while((line = reader.readLine()) != null) {
          Matcher m = p.matcher(line);
          if (!m.matches()) {
            continue;
          }

          List<Column> columns = new ArrayList<Column>();
          for (int i = 5; i < args.length; i++) {
            columns.add(new Column(args[i], m.group(i - 4)));
          }

          Record record = new Record();
          record.setRecordId(UUID.randomUUID().toString());
          record.setFamily("cf1");
          record.setColumns(columns);

          RecordMutation recordMutation = new RecordMutation();
          recordMutation.setRecord(record);
          recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);

          List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
          recordMutations.add(recordMutation);

          RowMutation mutation = new RowMutation();
          mutation.setTable(tablename);
          mutation.setRowId(UUID.randomUUID().toString());
          mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
          mutation.setRecordMutations(recordMutations);

          mutations.add(mutation);

          if (mutations.size() == batchSize) {
            client.mutateBatch(mutations);
            mutations.clear();
          }
        }
        if (mutations.size() > 0) {
          client.mutateBatch(mutations);
          mutations.clear();
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new CommandException(e.getMessage());
    }
  }

  @Override
  public String help() {
    return "index an access log, args; file batchsize tablename regex colnames+";
  }
}
