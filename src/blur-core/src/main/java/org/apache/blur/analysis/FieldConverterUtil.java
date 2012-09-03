package org.apache.blur.analysis;
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
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

public class FieldConverterUtil {

  private static final String LONG = "long";

  /**
   * This method runs the converter on each of the fields in the document and
   * returns the a new document.
   * 
   * @param document
   *          the original document.
   * @param converter
   *          the converter.
   * @return the original document.
   */
  public static Document convert(Document document, FieldConverter converter) {
    List<Fieldable> fields = document.getFields();
    int size = fields.size();
    for (int i = 0; i < size; i++) {
      Fieldable origField = fields.get(i);
      Fieldable newField = converter.convert(origField);
      if (newField != null) {
        fields.set(i, newField);
      }
    }
    return document;
  }

  public static Document convert(Document document, BlurAnalyzer analyzer) {
    List<Fieldable> fields = document.getFields();
    int size = fields.size();
    for (int i = 0; i < size; i++) {
      Fieldable origField = fields.get(i);
      FieldConverter converter = analyzer.getFieldConverter(origField.name());
      if (converter != null) {
        Fieldable newField = converter.convert(origField);
        if (newField != null) {
          fields.set(i, newField);
        }
      }
    }
    return document;
  }

  public static boolean isType(String type) {
    if (type.startsWith(LONG)) {
      return true;
    }
    return false;
  }

  public static Analyzer getAnalyzer(String type) {
    if (type.startsWith(LONG)) {
      return new LongAnalyzer(type);
    }
    throw new RuntimeException("Type [" + type + "] not found.");
  }

}
