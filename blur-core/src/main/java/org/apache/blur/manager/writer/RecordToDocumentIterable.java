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
package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.thrift.generated.Record;
import org.apache.lucene.document.Field;

public class RecordToDocumentIterable implements Iterable<Iterable<Field>> {

  private long _count;
  private final IterableRow _row;
  private final FieldManager _fieldManager;

  public RecordToDocumentIterable(IterableRow row, FieldManager fieldManager) {
    _row = row;
    _fieldManager = fieldManager;
  }

  @Override
  public Iterator<Iterable<Field>> iterator() {
    final Iterator<Record> iterator = _row.iterator();
    return new Iterator<Iterable<Field>>() {
      private long count = 0;

      @Override
      public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          _count = count;
        }
        return hasNext;
      }

      @Override
      public Iterable<Field> next() {
        Record record = iterator.next();
        count++;
        try {
          return convert(record);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new RuntimeException("Not Supported.");
      }
    };
  }

  public Iterable<Field> convert(Record record) throws IOException {
    return _fieldManager.getFields(_row.getRowId(), record);
  }

  public long count() {
    return _count;
  }

}
