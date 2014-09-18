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

import java.util.Map;

import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

public class ExampleType extends CustomFieldTypeDefinition {

  private String _fieldNameForThisInstance;

  /**
   * Get the name of the type.
   * 
   * @return the name.
   */
  @Override
  public String getName() {
    return "example";
  }

  /**
   * Configures this instance for the type.
   * 
   * @param fieldNameForThisInstance
   *          the field name for this instance.
   * @param properties
   *          the properties passed into this type definition from the
   *          {@link Blur.Iface#addColumnDefinition(String, org.apache.blur.thrift.generated.ColumnDefinition)}
   *          method.
   */
  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    _fieldNameForThisInstance = fieldNameForThisInstance;
  }

  /**
   * Create {@link Field}s for the index as well as for storing the original
   * data for retrieval.
   * 
   * @param family
   *          the family name.
   * @param column
   *          the column that holds the name and value.
   * 
   * @return the {@link Iterable} of {@link Field}s.
   */
  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = family + "." + column.getName();
    String value = column.getValue();
    return makeIterable(new StringField(name, value, Store.YES));
  }

  /**
   * Create {@link Field}s for the index do NOT store the data because the is a
   * sub column.
   * 
   * @param family
   *          the family name.
   * @param column
   *          the column that holds the name and value.
   * @param subName
   *          the sub column name.
   * 
   * @return the {@link Iterable} of {@link Field}s.
   */
  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = family + "." + column.getName() + "." + subName;
    String value = column.getValue();
    return makeIterable(new StringField(name, value, Store.NO));
  }

  /**
   * Gets the query from the text provided by the query parser.
   * 
   * @param text
   *          the text provided by the query parser.
   * @return the {@link Query}.
   */
  @Override
  public Query getCustomQuery(String text) {
    return new TermQuery(new Term(_fieldNameForThisInstance, text));
  }
  
  @Override
  public SortField getSortField(boolean reverse) {
    throw new RuntimeException("Sort not supported.");
  }

  @Override
  public String readTerm(BytesRef byteRef) {
	return byteRef.utf8ToString();
  }

}
