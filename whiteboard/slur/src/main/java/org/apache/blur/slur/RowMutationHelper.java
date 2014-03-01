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
package org.apache.blur.slur;

import java.util.Collection;
import java.util.List;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.google.common.collect.Lists;

public class RowMutationHelper {

  public static List<RowMutation> from(Collection<SolrInputDocument> docs, String table) {
    List<RowMutation> mutations = Lists.newArrayList();
    for(SolrInputDocument d: docs) {
      mutations.add(from(d, table));
    }
    return mutations;
  }
  
  public static RowMutation from(SolrInputDocument doc, String table) {
    validate(doc);

    RowMutation mutate = new RowMutation();
    String rowid = extractId(doc);
    mutate.setRowId(rowid);
    mutate.setTable(table);
    List<RecordMutation> recordMutations = Lists.newArrayList();
    if (doc.hasChildDocuments()) {
      for(SolrInputDocument child: doc.getChildDocuments()) {
        recordMutations.add(createRecordMutation(child, extractId(child)));
      }
    } else {
      recordMutations.add(createRecordMutation(doc, rowid));
    }
    mutate.setRecordMutations(recordMutations);
    return mutate;
  }

  private static String extractId(SolrInputDocument doc) {
    Object id = doc.getFieldValue("rowid");
    if (id == null) {
      id = doc.getFieldValue("id");
    }
    if (id == null) {
      throw new IllegalArgumentException("Document must either have id or rowid field.");
    }
    return id.toString();
  }

  private static RecordMutation createRecordMutation(SolrInputDocument doc, String id) {
    RecordMutation recordMutation = new RecordMutation();
    // TODO: what's solr default behavior?
    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
    Record record = new Record();
    record.setFamily(findFamily(doc));
    record.setRecordId(id);

    for (String fieldName : doc.getFieldNames()) {
      if (!fieldName.contains(".")) {
        continue;
      }
      SolrInputField field = doc.getField(fieldName);
      String rawColumnName = fieldName.substring(fieldName.indexOf(".") + 1, fieldName.length());

      if (field.getValueCount() > 1) {
        for (Object fieldVal : field.getValues()) {
          record.addToColumns(new Column(rawColumnName, fieldVal.toString()));
        }
      } else {
        record.addToColumns(new Column(rawColumnName, field.getFirstValue().toString()));
      }
    }
    recordMutation.setRecord(record);
    return recordMutation;
  }

  private static String findFamily(SolrInputDocument doc) {
    for (String name : doc.getFieldNames()) {
      if (name.contains(".")) {
        return name.substring(0, name.indexOf("."));
      }
    }
    throw new IllegalArgumentException("Unable to determine column family from document");
  }

  private static void validate(SolrInputDocument doc) {
    if ((doc.getFieldNames().size() > 1) && (doc.hasChildDocuments())) {

    }

  }


}
