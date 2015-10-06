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
package org.apache.blur.lucene.security.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

public abstract class AccessControlWriter {

  /**
   * Adds document read visibility rule to document.
   */
  public abstract Iterable<IndexableField> addReadVisiblity(String read, Iterable<IndexableField> fields);

  /**
   * Adds document discover visibility rule to document.
   */
  public abstract Iterable<IndexableField> addDiscoverVisiblity(String discover, Iterable<IndexableField> fields);

  /**
   * Adds a read mask to document. If a field has been masked the value can not
   * be viewed, but if a search utilizes the tokens from the field the document
   * can be found.
   */
  public abstract Iterable<IndexableField> addReadMask(String fieldToMask, Iterable<IndexableField> fields);

  /**
   * This method should be called as the document is being added to the index
   * writer.
   * 
   * @param fields
   * @return
   */
  public abstract Iterable<IndexableField> lastStepBeforeIndexing(Iterable<IndexableField> fields);

  protected Iterable<IndexableField> addField(Iterable<IndexableField> fields, IndexableField... fieldsToAdd) {
    if (fields instanceof Document) {
      Document document = (Document) fields;
      if (fieldsToAdd != null) {
        for (IndexableField field : fieldsToAdd) {
          document.add(field);
        }
      }
      return document;
    }
    List<IndexableField> list = new ArrayList<IndexableField>();
    for (IndexableField indexableField : fields) {
      list.add(indexableField);
    }
    if (fieldsToAdd != null) {
      for (IndexableField field : fieldsToAdd) {
        list.add(field);
      }
    }
    return list;
  }

}
