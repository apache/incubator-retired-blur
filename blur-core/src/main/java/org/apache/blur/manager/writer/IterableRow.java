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

import java.util.Iterator;

import org.apache.blur.thrift.generated.Record;

public class IterableRow implements Iterable<Record> {

  public static final Iterable<Record> EMPTY = new Iterable<Record>() {
    @Override
    public Iterator<Record> iterator() {
      return new Iterator<Record>() {
        @Override
        public void remove() {
          throw new RuntimeException("Not Supported.");
        }

        @Override
        public Record next() {
          throw new RuntimeException("Empty");
        }

        @Override
        public boolean hasNext() {
          return false;
        }
      };
    }
  };
  private final String _rowId;
  private final Iterable<Record> _records;

  public IterableRow(String rowId, Iterable<Record> records) {
    _rowId = rowId;
    _records = records;
  }

  public String getRowId() {
    return _rowId;
  }

  @Override
  public Iterator<Record> iterator() {
    return _records.iterator();
  }

}
