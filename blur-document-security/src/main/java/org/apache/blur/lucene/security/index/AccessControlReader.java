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

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.Filter;

public abstract class AccessControlReader implements Cloneable {

  public final boolean hasAccess(ReadType type, int docID) throws IOException {
    switch (type) {
    
    case LIVEDOCS:
      return readOrDiscoverAccess(docID);
    case DOCUMENT_FETCH_DISCOVER:
      return discoverAccess(docID);
    case DOCS_ENUM:
    case TERMS_ENUM:
    case BINARY_DOC_VALUE:
    case DOCUMENT_FETCH_READ:
    case NORM_VALUE:
    case NUMERIC_DOC_VALUE:
    case SORTED_DOC_VALUE:
    case SORTED_SET_DOC_VALUE:
      return readAccess(docID);
    default:
      throw new IOException("Unknown type [" + type + "]");
    }
  }

  protected abstract boolean readAccess(int docID) throws IOException;

  protected abstract boolean discoverAccess(int docID) throws IOException;

  protected abstract boolean readOrDiscoverAccess(int docID) throws IOException;

  public abstract boolean canDiscoverField(String name) throws IOException;

  public abstract AccessControlReader clone(AtomicReader in) throws IOException;

  public abstract Filter getQueryFilter() throws IOException;

}
