package org.apache.blur.lucene.serializer;

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

import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.lucene.search.SuperQuery;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class ProtoSerializer {

  public static void main(String[] args) throws ParseException, IOException {

    QueryParser parser = new QueryParser(Version.LUCENE_40, "", new StandardAnalyzer(Version.LUCENE_40));

    Query query = parser.parse("a:v1 b:v2 c:v3~ c:asda*asda");
    
    SuperQuery superQuery = new SuperQuery(query,ScoreType.SUPER,new Term("_primedoc_"));

    QueryWritable queryWritable = new QueryWritable(superQuery);
    DataOutputBuffer buffer = new DataOutputBuffer();
    queryWritable.write(buffer);
    buffer.close();

    System.out.println(new String(buffer.getData(), 0, buffer.getLength()));

    QueryWritable qw = new QueryWritable();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(buffer.getData(), 0, buffer.getLength());
    qw.readFields(in);

    System.out.println("------------");
    
    System.out.println(qw.getQuery());
    
    System.out.println("------------");

    while (true) {
      run(superQuery);
    }
  }

  private static void run(Query query) throws IOException {

    DataOutputBuffer buffer = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();
    QueryWritable outQw = new QueryWritable();
    QueryWritable inQw = new QueryWritable();

    long s = System.nanoTime();
    int count = 100000;
    for (int i = 0; i < count; i++) {
      outQw.setQuery(query);
      outQw.write(buffer);

      in.reset(buffer.getData(), 0, buffer.getLength());
      inQw.readFields(in);

      buffer.reset();
    }
    long e = System.nanoTime();
    System.out.println((e - s) / 1000000.0 / (double) count);
    // System.out.println((e - s) / (double) count);
  }
}
