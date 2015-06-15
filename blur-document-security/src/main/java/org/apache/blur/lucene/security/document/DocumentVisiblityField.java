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
package org.apache.blur.lucene.security.document;

import java.io.IOException;

import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.blur.lucene.security.analysis.DocumentVisibilityTokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;

public class DocumentVisiblityField extends Field implements IndexableField {

  public static final FieldType TYPE_NOT_STORED = new FieldType();
  public static final FieldType TYPE_STORED = new FieldType();

  static {
    boolean tokenized = true;
    TYPE_NOT_STORED.setIndexed(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE_NOT_STORED.setTokenized(tokenized);
    TYPE_NOT_STORED.freeze();

    TYPE_STORED.setIndexed(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE_STORED.setStored(true);
    TYPE_STORED.setTokenized(tokenized);
    TYPE_STORED.freeze();
  }

  private DocumentVisibilityTokenStream _visibilityTokenStream;

  public DocumentVisiblityField(String name, String visibility) {
    this(name, visibility, Store.YES);
  }

  public DocumentVisiblityField(String name, String visibility, Store store) {
    this(name, new DocumentVisibility(visibility), store);
  }

  public DocumentVisiblityField(String name, DocumentVisibility visibility) {
    super(name, DocumentVisibilityTokenStream.toString(visibility.flatten()), TYPE_STORED);
  }

  public DocumentVisiblityField(String name, DocumentVisibility visibility, Store store) {
    super(name, DocumentVisibilityTokenStream.toString(visibility.flatten()), store == Store.YES ? TYPE_STORED
        : TYPE_NOT_STORED);
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer) throws IOException {
    if (!(_visibilityTokenStream instanceof DocumentVisibilityTokenStream)) {
      _visibilityTokenStream = new DocumentVisibilityTokenStream(stringValue());
    }
    return _visibilityTokenStream;
  }

}