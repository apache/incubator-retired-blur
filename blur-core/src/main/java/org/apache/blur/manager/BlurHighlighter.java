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
package org.apache.blur.manager;

import java.io.IOException;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.HighlightHelper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;

public class BlurHighlighter {

  private final String _preTag;
  private final String _postTag;
  private final Query _highlightQuery;
  private final FieldManager _fieldManager;
  private final boolean _shouldHighlight;

  public BlurHighlighter(Query highlightQuery, FieldManager fieldManager, Selector selector) {
    HighlightOptions highlightOptions = selector.getHighlightOptions();
    if (highlightOptions != null) {
      _preTag = highlightOptions.getPreTag();
      _postTag = highlightOptions.getPostTag();
      _highlightQuery = highlightQuery;
      _fieldManager = fieldManager;
      _shouldHighlight = true;
    } else {
      _preTag = null;
      _postTag = null;
      _highlightQuery = null;
      _fieldManager = null;
      _shouldHighlight = false;
    }
  }

  public BlurHighlighter() {
    _preTag = null;
    _postTag = null;
    _highlightQuery = null;
    _fieldManager = null;
    _shouldHighlight = false;
  }

  public boolean shouldHighlight() {
    return _shouldHighlight;
  }

  public String getPreTag() {
    return _preTag;
  }

  public String getPostTag() {
    return _postTag;
  }

  public Query getHighlightQuery() {
    return _highlightQuery;
  }

  public FieldManager getFieldManager() {
    return _fieldManager;
  }

  public Document highlight(int docID, Document document, SegmentReader segmentReader) throws IOException {
    Document highlight;
    try {
      highlight = HighlightHelper.highlight(docID, document, _highlightQuery, _fieldManager, segmentReader, _preTag,
          _postTag);
    } catch (InvalidTokenOffsetsException e) {
      throw new IOException(e);
    }
    return highlight;
  }

}
