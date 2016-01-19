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
package org.apache.blur.command;

import java.io.IOException;

import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public class InfiniteLoopCommand extends IndexReadCommandSingleTable<Boolean> {

  @Override
  public Boolean execute(IndexContext context) throws IOException, InterruptedException {
    try {
      IndexReader indexReader = context.getIndexReader();
      while (true) {
        long hash = 0;
        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
          AtomicReader reader = atomicReaderContext.reader();
          for (String field : reader.fields()) {
            Terms terms = reader.terms(field);
            BytesRef bytesRef;
            TermsEnum iterator = terms.iterator(null);
            while ((bytesRef = iterator.next()) != null) {
              hash += bytesRef.hashCode();
            }
          }
        }
        System.out.println("hashcode = " + hash);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Throwable t) {
      t.printStackTrace();
      if (t instanceof InterruptedException) {
        throw t;
      } else if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      }
      throw new RuntimeException(t);
    }
  }

  @Override
  public String getName() {
    return "infinite-loop";
  }

}
