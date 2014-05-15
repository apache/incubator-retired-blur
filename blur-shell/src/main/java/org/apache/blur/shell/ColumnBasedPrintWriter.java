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
package org.apache.blur.shell;

import java.io.PrintWriter;
import java.util.Arrays;

public class ColumnBasedPrintWriter extends PagingPrintWriter {

  private static final String SEP = "|";
  private final int[] _columnWidths;

  public ColumnBasedPrintWriter(PrintWriter out, int numberOfColumns, int maxColumnWidth) {
    super(out);
    int[] cols = new int[numberOfColumns];
    Arrays.fill(cols, maxColumnWidth);
    _columnWidths = cols;
  }

  public ColumnBasedPrintWriter(PrintWriter out, int[] columnWidths) {
    super(out);
    _columnWidths = columnWidths;
  }

  public void println(Object... x) throws FinishedException {
    for (int i = 0; i < _columnWidths.length; i++) {
      if (i != 0) {
        print(SEP);
      }
      if (x != null && i < x.length) {
        Object o = x[i];
        String s = truncate(getStr(o), _columnWidths[i]);
        print(s);
      }
    }
    println();
  }

  private String truncate(String str, int maxColumnWidth) {
    if (str.length() > maxColumnWidth) {
      return str.substring(0, maxColumnWidth - 3) + "...";
    }
    return buffer(str, maxColumnWidth);
  }

  private String buffer(String str, int maxColumnWidth) {
    StringBuilder builder = new StringBuilder(str);
    while (builder.length() < maxColumnWidth) {
      builder.append(' ');
    }
    return builder.toString();
  }

  private String getStr(Object o) {
    if (o == null) {
      return "NULL";
    }
    return o.toString();
  }

}
