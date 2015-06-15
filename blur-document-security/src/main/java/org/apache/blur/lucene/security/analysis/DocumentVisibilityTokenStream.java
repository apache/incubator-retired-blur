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
package org.apache.blur.lucene.security.analysis;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class DocumentVisibilityTokenStream extends TokenStream {

  private static final String UTF_8 = "UTF-8";

  private final String _visiblity;
  private final CharTermAttribute _tokenAtt;
  private final int _length;
  
  private int _position;
  private boolean more = true;

  public DocumentVisibilityTokenStream(String visibility) {
    _tokenAtt = addAttribute(CharTermAttribute.class);
    _visiblity = visibility;
    _length = _visiblity.length();
  }

  public DocumentVisibilityTokenStream(DocumentVisibility visibility) {
    this(toString(visibility.flatten()));
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (more) {
      int next = findNextPipe(_position);
      if (next < 0) {
        more = false;
        next = _length;
      }
      _tokenAtt.setLength(0);
      if (hasSurroundingParens(next)) {
        _tokenAtt.append(_visiblity, _position + 1, next - 1);
      } else {
        _tokenAtt.append(_visiblity, _position, next);
      }
      _position = next + 1;
      return true;
    }
    return false;
  }

  private boolean hasSurroundingParens(int end) {
    if (_visiblity.charAt(_position) == '(') {
      if (_visiblity.charAt(end - 1) == ')') {
        return true;
      } else {
        throw new RuntimeException("This should never happen");
      }
    }
    return false;
  }

  private int findNextPipe(int position) {
    int p = 0;
    for (int i = position; i < _length; i++) {
      char c = _visiblity.charAt(i);
      switch (c) {
      case '(':
        p++;
        break;
      case ')':
        p--;
        break;
      case '|':
        if (p == 0) {
          return i;
        }
      default:
        break;
      }
    }
    return -1;
  }

  public static String toString(byte[] bs) {
    try {
      return new String(bs, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}