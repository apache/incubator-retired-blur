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
package org.apache.blur.analysis.type.spatial;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public abstract class BaseSpatialFieldTypeDefinitionTest {

  protected Directory _dir = new RAMDirectory();

  public void runGisTypeTest() throws IOException, ParseException {
    BaseFieldManager fieldManager = getFieldManager(new NoStopWordStandardAnalyzer());
    setupGisField(fieldManager);

    Record record = new Record();
    record.setFamily("fam");
    record.setRecordId("1234");
    record.addToColumns(new Column("geo", "77.4011984,39.040444"));

    List<Field> fields = fieldManager.getFields("1234", record);

    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, fieldManager.getAnalyzerForIndex());

    IndexWriter writer = new IndexWriter(_dir, conf);
    fields.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
    writer.addDocument(fields);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(_dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    SuperParser parser = new SuperParser(Version.LUCENE_43, fieldManager, true, null, ScoreType.SUPER, new Term(
        BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE));

    Query query = parser.parse("fam.geo:\"Intersects(Circle(77.4011984,39.040444 d=10.0m))\"");

    TopDocs topDocs = searcher.search(query, 10);

    assertEquals(1, topDocs.totalHits);

    reader.close();

  }

  protected void runGisDocValueTest(String s) throws IOException {
    DirectoryReader reader = DirectoryReader.open(_dir);
    AtomicReader atomicReader = reader.leaves().get(0).reader();
    SortedDocValues sortedDocValues = atomicReader.getSortedDocValues("fam.geo");
    BytesRef result = new BytesRef();
    sortedDocValues.get(0, result);
    assertEquals(s, result.utf8ToString());
    System.out.println(result.utf8ToString());
    reader.close();
  }

  protected abstract void setupGisField(BaseFieldManager fieldManager) throws IOException;

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

}
