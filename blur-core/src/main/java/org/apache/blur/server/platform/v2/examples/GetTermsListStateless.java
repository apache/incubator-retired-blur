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
package org.apache.blur.server.platform.v2.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;

import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.platform.v2.Arguments;
import org.apache.blur.server.platform.v2.Client;
import org.apache.blur.server.platform.v2.ControllerCommand;
import org.apache.blur.server.platform.v2.ControllerCommandContext;
import org.apache.blur.server.platform.v2.ReadCommand;
import org.apache.blur.server.platform.v2.Session;
import org.apache.blur.server.platform.v2.ShardCommand;
import org.apache.blur.server.platform.v2.ShardCommandContext;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public class GetTermsListStateless {
  
  private static final String FETCH = "FETCH";
  private static final String STARTS_WITH = "startsWith";
  private static final String FIELD = "field";

  enum ACTION {
    CURRENT, ADVANCE, NEXT
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    
    // Index logic

    final ReadCommand<List<BytesRef>> readCommand = new ReadCommand<List<BytesRef>>() {
      @Override
      public List<BytesRef> execute(Arguments arguments, Session session, ShardCommandContext context, IndexSearcherClosable searcher)
          throws BlurException, IOException {
        String field = arguments.get(FIELD);
        BytesRef startsWith = arguments.get(STARTS_WITH);
        int fetch = arguments.get(FETCH);

        IndexReader indexReader = searcher.getIndexReader();
        AtomicReader reader = SlowCompositeReaderWrapper.wrap(indexReader);
        Terms terms = reader.fields().terms(field);
        TermsEnum termsEnum = terms.iterator(null);
        termsEnum.seekCeil(startsWith);

        List<BytesRef> results = new ArrayList<BytesRef>();
        results.add(termsEnum.term());
        BytesRef ref;
        while ((ref = termsEnum.next()) != null) {
          if (results.size() >= fetch) {
            return results;
          }
          results.add(ref);
        }
        return results;
      }
    };
    
    // Shard logic

    final ShardCommand<List<BytesRef>> shardCommand = new ShardCommand<List<BytesRef>>() {

      @Override
      public List<BytesRef> execute(Arguments arguments, Session session, Set<String> shardIdsToExecute,
          ShardCommandContext context) throws BlurException {
        Map<String, Future<List<BytesRef>>> execute = context.execute(shardIdsToExecute, arguments, session,
            readCommand);
        Map<String, List<BytesRef>> values = context.getValues(execute);
        SortedSet<BytesRef> resultSet = new TreeSet<BytesRef>();
        for (List<BytesRef> v : values.values()) {
          resultSet.addAll(v);
        }
        List<BytesRef> results = new ArrayList<BytesRef>();
        results.addAll(resultSet);
        int size = results.size();
        int fetch = arguments.get(FETCH);
        return new ArrayList<BytesRef>(results.subList(0, Math.min(size, fetch)));
      }
    };
    
    // Controller logic

    ControllerCommand<List<BytesRef>> controllerCommand = new ControllerCommand<List<BytesRef>>() {
      @Override
      public List<BytesRef> execute(Arguments arguments, Session session, String table, ControllerCommandContext context)
          throws BlurException, IOException {
        Set<String> shardIdsToExecute = context.getAllShardIdsForTable(table);
        Map<String, Future<List<BytesRef>>> execute = context.execute(shardIdsToExecute, arguments, session,
            shardCommand);
        Map<String, List<BytesRef>> values = context.getValues(execute);

        SortedSet<BytesRef> resultSet = new TreeSet<BytesRef>();
        for (List<BytesRef> v : values.values()) {
          resultSet.addAll(v);
        }
        List<BytesRef> results = new ArrayList<BytesRef>();
        results.addAll(resultSet);
        int size = results.size();
        int fetch = arguments.get(FETCH);
        return new ArrayList<BytesRef>(results.subList(0, Math.min(size, fetch)));
      }
    };

    Client client = new Client();
    Arguments arguments = new Arguments().set(STARTS_WITH, new BytesRef("abc"));
    List<BytesRef> refs;
    while (!(refs = client.execute(arguments, controllerCommand)).isEmpty()) {
      BytesRef last = null;
      for (BytesRef ref : refs) {
        last = ref;
        System.out.println(ref.utf8ToString());
      }
      arguments.set(STARTS_WITH, last);
    }

  }
}
