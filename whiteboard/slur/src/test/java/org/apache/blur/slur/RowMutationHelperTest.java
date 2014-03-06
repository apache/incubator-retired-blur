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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RowMutationHelperTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void basicOneForOneConversion() {
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("rowid", "123");

    SolrInputDocument child = new SolrInputDocument();
    child.addField("recordid", "1");
    child.addField("fam.key", "value");

    parent.addChildDocument(child);

    RowMutation mutate = RowMutationHelper.from(parent, "foo");

    assertEquals("Should get our rowid back.", "123", mutate.getRowId());
    assertEquals("Should get a single record.", 1, mutate.getRecordMutationsSize());
    assertEquals("Tablename should be set", "foo", mutate.getTable());
  }

  @Test
  public void multivalueFieldsShouldTranslate() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("rowid", "123");

    SolrInputDocument child = new SolrInputDocument();
    child.addField("recordid", "1");
    child.addField("fam.key", "value1");
    child.addField("fam.key", "value2");
    doc.addChildDocument(child);
    RowMutation mutate = RowMutationHelper.from(doc, "foo");

    List<Object> vals = getRecordValues("key", mutate);
    assertEquals("Should get both values back.", 2, vals.size());

    assertEquals("value1", vals.get(0).toString());
    assertEquals("value2", vals.get(1).toString());
  }

  @Test
  public void documentWithChildDocumentsShouldBeRowWithRecords() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("rowid", "1");

    List<SolrInputDocument> children = Lists.newArrayList();

    for (int i = 0; i < 10; i++) {
      SolrInputDocument child = new SolrInputDocument();
      child.addField("recordid", i);
      child.addField("fam.key", "value" + i);
      children.add(child);
    }
    doc.addChildDocuments(children);

    RowMutation mutate = RowMutationHelper.from(doc, "foo");

    assertEquals("Children should turn into records.", 10, mutate.getRecordMutationsSize());
    assertEquals("Should get a simple value back.", "value0", getRecordValue("key", mutate));
    assertEquals("Should properly figure our family.", "fam", getFirstRecord(mutate).getFamily());
  }

  @Test(expected = IllegalArgumentException.class)
  public void docWithChildrenCantItselfHaveFieldValues() {
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("id", "1");
    parent.addField("fam.key1", "123");
    SolrInputDocument child = new SolrInputDocument();
    parent.addChildDocument(child);

    RowMutationHelper.from(parent, "foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rowsCantDirectlyHaveValues() {
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("id", "1");
    parent.addField("fam.key1", "123");

    RowMutationHelper.from(parent, "foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void docsMustUseTheNormalBlurFamilyColumnFormat() {
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("columnWithoutFamily", "123");
    SolrInputDocument child = new SolrInputDocument();
    parent.addChildDocument(child);

    RowMutationHelper.from(parent, "foo");
  }

  private Object getRecordValue(String field, RowMutation mutate) {
    return getRecordValues(field, mutate).get(0);
  }

  private List<Object> getRecordValues(String field, RowMutation mutate) {
    List<Object> vals = Lists.newArrayList();
    Record rec = getFirstRecord(mutate);

    for (Column col : rec.getColumns()) {

      if (col.getName().equals(field)) {
        vals.add(col.getValue());
      }
    }
    return vals;
  }

  private Record getFirstRecord(RowMutation mutate) {
    return mutate.getRecordMutations().get(0).getRecord();
  }

}
