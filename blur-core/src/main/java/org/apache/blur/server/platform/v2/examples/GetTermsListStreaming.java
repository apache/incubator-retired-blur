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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.platform.v2.Arguments;
import org.apache.blur.server.platform.v2.Client;
import org.apache.blur.server.platform.v2.ControllerCommand;
import org.apache.blur.server.platform.v2.ControllerCommandContext;
import org.apache.blur.server.platform.v2.ReadCommand;
import org.apache.blur.server.platform.v2.Session;
import org.apache.blur.server.platform.v2.ShardCommand;
import org.apache.blur.server.platform.v2.ShardCommandContext;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public class GetTermsListStreaming {
  private static final String STARTS_WITH = "startsWith";
  private static final String FIELD_TYPE_DEFINITION = "fieldTypeDefinition";
  private static final String FIELD = "field";
  private static final String TERMS_ENUM = "termsEnum";
  private static final String CURRENT_TERM = "currentTerm";
  private static final String TERM = "term";
  private static final String ACTION_STR = "action";

  enum ACTION {
    CURRENT, ADVANCE, NEXT
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {

    // Index logic

    final ReadCommand<BytesRef> readCommand = new ReadCommand<BytesRef>() {

      @Override
      public void setup(Arguments arguments, Session session, ShardCommandContext context,
          IndexSearcherClosable searcher) throws BlurException, IOException {
        TableContext tableContext = context.getTableContext();
        String field = arguments.get(FIELD);

        FieldManager fieldManager = tableContext.getFieldManager();
        FieldTypeDefinition fieldTypeDefinition = fieldManager.getFieldTypeDefinition(field);

        IndexReader indexReader = searcher.getIndexReader();
        AtomicReader reader = SlowCompositeReaderWrapper.wrap(indexReader);
        Terms terms = reader.fields().terms(field);
        TermsEnum termsEnum = terms.iterator(null);

        BytesRef ref = arguments.get(STARTS_WITH);

        termsEnum.seekCeil(ref);

        session.set(FIELD, field);
        session.set(TERMS_ENUM, termsEnum);
        session.set(FIELD_TYPE_DEFINITION, fieldTypeDefinition);
      }

      @Override
      public BytesRef execute(Arguments arguments, Session session, ShardCommandContext context,
          IndexSearcherClosable searcher) throws BlurException, IOException {
        TermsEnum termsEnum = session.get(TERMS_ENUM);
        ACTION action = arguments.get(ACTION_STR);
        BytesRef current = termsEnum.term();
        switch (action) {
        case CURRENT:
          return current;
        case ADVANCE: {
          BytesRef t = arguments.get(TERM);
          if (t.compareTo(current) <= 0) {
            return current;
          }
          termsEnum.seekCeil(t);
          return termsEnum.term();
        }
        case NEXT: {
          throw new RuntimeException("Not impl yet.");
        }
        default:
          throw new BException("Unknown action [{0}]", action);
        }
      }
    };

    // Shard logic

    final ShardCommand<BytesRef> shardCommand = new ShardCommand<BytesRef>() {

      @Override
      public void setup(Arguments arguments, Session session, Set<String> shardIdsToExecute, ShardCommandContext context)
          throws BlurException, IOException {
        context.setup(arguments, session, shardIdsToExecute, readCommand);
        Arguments argument = new Arguments().set(ACTION_STR, ACTION.CURRENT);
        BytesRef lowest = executeAndGetLowestResult(readCommand, session, shardIdsToExecute, context, argument);
        session.set(CURRENT_TERM, lowest);
      }

      private BytesRef executeAndGetLowestResult(final ReadCommand<BytesRef> readCommand, Session session,
          Set<String> shardIdsToExecute, ShardCommandContext context, Arguments argument) {
        Map<String, Future<BytesRef>> results = context.execute(shardIdsToExecute, argument, session, readCommand);
        Map<String, BytesRef> values = context.getValues(results);
        // get lowest term
        BytesRef lowest = null;
        for (BytesRef bytesRef : values.values()) {
          if (bytesRef == null) {
            continue;
          } else if (lowest == null || bytesRef.compareTo(lowest) < 0) {
            lowest = bytesRef;
          }
        }
        return lowest;
      }

      @Override
      public BytesRef execute(Arguments arguments, Session session, Set<String> shardIdsToExecute,
          ShardCommandContext context) throws BlurException {
        BytesRef current = session.get(CURRENT_TERM);

        BytesRef term = arguments.get(TERM);
        ACTION action = arguments.get(ACTION_STR);

        switch (action) {
        case CURRENT: {
          return current;
        }
        case ADVANCE: {
          Arguments argument = new Arguments().set(ACTION_STR, ACTION.ADVANCE).set(TERM, term);
          BytesRef lowest = executeAndGetLowestResult(readCommand, session, shardIdsToExecute, context, argument);
          return session.set(CURRENT_TERM, lowest);
        }
        case NEXT: {
          throw new RuntimeException("Not impl yet.");
        }
        default:
          throw new BException("Unknown action [{0}]", action);
        }
      }
    };

    // Controller logic

    ControllerCommand<BytesRef> controllerCommand = new ControllerCommand<BytesRef>() {

      @Override
      public void setup(Arguments arguments, Session session, String table, ControllerCommandContext context)
          throws BlurException, IOException {
        context.setup(arguments, session, table, readCommand);

        Set<String> shardIdsToExecute = context.getAllShardIdsForTable(table);
        Arguments argument = new Arguments().set(ACTION_STR, ACTION.CURRENT);
        BytesRef lowest = executeAndGetLowestResult(readCommand, session, shardIdsToExecute, context, argument);
        session.set(CURRENT_TERM, lowest);
      }

      @Override
      public BytesRef execute(Arguments arguments, Session session, String table, ControllerCommandContext context)
          throws BlurException, IOException {
        Set<String> shardIdsToExecute = context.getAllShardIdsForTable(table);

        BytesRef current = session.get(CURRENT_TERM);

        BytesRef term = arguments.get(TERM);
        ACTION action = arguments.get(ACTION_STR);

        switch (action) {
        case CURRENT: {
          return current;
        }
        case ADVANCE: {
          Arguments argument = new Arguments().set(ACTION_STR, ACTION.ADVANCE).set(TERM, term);
          BytesRef lowest = executeAndGetLowestResult(readCommand, session, shardIdsToExecute, context, argument);
          return session.set(CURRENT_TERM, lowest);
        }
        case NEXT: {
          throw new RuntimeException("Not impl yet.");
        }
        default:
          throw new BException("Unknown action [{0}]", action);
        }

      }

      private BytesRef executeAndGetLowestResult(final ReadCommand<BytesRef> readCommand, Session session,
          Set<String> shardIdsToExecute, ControllerCommandContext context, Arguments argument) {
        Map<String, Future<BytesRef>> results = context.execute(shardIdsToExecute, argument, session, shardCommand);
        Map<String, BytesRef> values = context.getValues(results);
        // get lowest term
        BytesRef lowest = null;
        for (BytesRef bytesRef : values.values()) {
          if (bytesRef == null) {
            continue;
          } else if (lowest == null || bytesRef.compareTo(lowest) < 0) {
            lowest = bytesRef;
          }
        }
        return lowest;
      }
    };

    Client client = new Client();
    client.setup(new Arguments().set(FIELD, "f1").set(STARTS_WITH, new BytesRef("abc")), controllerCommand);

    Arguments arguments = new Arguments().set(ACTION_STR, ACTION.NEXT);
    BytesRef ref;
    while ((ref = client.execute(arguments, controllerCommand)) != null) {
      System.out.println(ref.utf8ToString());
    }

  }
}
