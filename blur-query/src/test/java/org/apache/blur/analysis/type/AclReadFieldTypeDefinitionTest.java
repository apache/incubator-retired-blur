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
package org.apache.blur.analysis.type;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import lucene.security.index.AccessControlFactory;
import lucene.security.index.FilterAccessControlFactory;
import lucene.security.search.SecureIndexSearcher;

import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.lucene.search.SuperParser;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class AclReadFieldTypeDefinitionTest {

  private static final String FAM = "fam";
  private static final String FAM2 = "fam2";

  private Directory _dir = new RAMDirectory();
  private AccessControlFactory _accessControlFactory = new FilterAccessControlFactory();

  private BaseFieldManager _fieldManager;

  @Before
  public void setup() throws IOException {
    _fieldManager = getFieldManager(new NoStopWordStandardAnalyzer());
    setupFieldManager(_fieldManager);

    List<List<Field>> docs = new ArrayList<List<Field>>();
    {
      Record record = new Record();
      record.setFamily(FAM);
      record.setRecordId("1234");
      record.addToColumns(new Column("string", "value"));
      record.addToColumns(new Column("read", "a&b"));
      List<Field> fields = _fieldManager.getFields("1234", record);
      fields.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      docs.add(fields);
    }
    {
      Record record = new Record();
      record.setFamily(FAM);
      record.setRecordId("5678");
      record.addToColumns(new Column("string", "value"));
      record.addToColumns(new Column("read", "a&c"));
      docs.add(_fieldManager.getFields("1234", record));
    }

    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, _fieldManager.getAnalyzerForIndex());
    IndexWriter writer = new IndexWriter(_dir, conf);
    writer.addDocuments(docs);
    writer.close();
  }

  @Test
  public void test1RowQuery() throws IOException, ParseException {
    test(0, true, null);
  }

  @Test
  public void test1RecordQuery() throws IOException, ParseException {
    test(0, false, null);
  }

  @Test
  public void test2RowQuery() throws IOException, ParseException {
    test(1, true, Arrays.asList("a", "b"));
  }

  @Test
  public void test2RecordQuery() throws IOException, ParseException {
    test(1, false, Arrays.asList("a", "b"));
  }

  @Test
  public void test3RowQuery() throws IOException, ParseException {
    test(1, true, Arrays.asList("a", "b", "c"));
  }

  @Test
  public void test3RecordQuery() throws IOException, ParseException {
    test(2, false, Arrays.asList("a", "b", "c"));
  }

  @Test
  public void test4RowQuery() throws IOException, ParseException {
    test(0, true, Arrays.asList("a"));
  }

  @Test
  public void test4RecordQuery() throws IOException, ParseException {
    test(0, false, Arrays.asList("a"));
  }

  private AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  private void setupFieldManager(BaseFieldManager fieldManager) throws IOException {
    fieldManager.addColumnDefinition(FAM, "string", null, false, "string", false, false, null);
    fieldManager.addColumnDefinition(FAM, "read", null, false, "acl-read", false, false, null);
    fieldManager.addColumnDefinition(FAM2, "string", null, false, "string", false, false, null);
    fieldManager.addColumnDefinition(FAM2, "read", null, false, "acl-read", false, false, null);
  }

  protected BaseFieldManager getFieldManager(Analyzer a) throws IOException {
    BaseFieldManager fieldManager = new BaseFieldManager(BlurConstants.SUPER, a, new Configuration()) {
      @Override
      protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) {
        return true;
      }

      @Override
      protected void tryToLoad(String fieldName) {

      }

      @Override
      protected List<String> getFieldNamesToLoad() throws IOException {
        return new ArrayList<String>();
      }
    };
    return fieldManager;
  }

  private void test(int expected, boolean rowQuery, Collection<String> readAuthorizations) throws IOException,
      ParseException {
    DirectoryReader reader = DirectoryReader.open(_dir);
    SuperParser parser = new SuperParser(Version.LUCENE_43, _fieldManager, rowQuery, null, ScoreType.SUPER, new Term(
        BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE));

    Query query = parser.parse("fam.string:value");

    Collection<String> discoverAuthorizations = null;
    Set<String> discoverableFields = null;
    IndexSearcher searcher = new SecureIndexSearcher(reader, getAccessControlFactory(), readAuthorizations,
        discoverAuthorizations, discoverableFields);

    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(expected, topDocs.totalHits);
    reader.close();
  }
}
