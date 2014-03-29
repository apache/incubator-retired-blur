/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class PagingPrintWriter {

  @SuppressWarnings("serial")
  public static class FinishedException extends Exception {
  }

  private static final String HIT_ANY_KEY_TO_CONTINUE = "< Hit any key to continue or q to quit>";
  private final PrintWriter _printWriter;
  private int _line = 0;
  private int _lineLimit = Integer.MAX_VALUE;
  private boolean _finished;

  public PagingPrintWriter(PrintWriter printWriter) {
    _printWriter = printWriter;
  }

  public void println(String s) throws FinishedException {
    _printWriter.println(s);
    _line++;
    flush();
  }

  public void println(Object o) throws FinishedException {
    _printWriter.println(o.toString());
    _line++;
    flush();
  }

  public void print(char c) throws FinishedException {
    _printWriter.print(c);
    flush();
  }

  public void print(String s) throws FinishedException {
    _printWriter.print(s);
    flush();
  }

  public void println() throws FinishedException {
    _printWriter.println();
    _line++;
    flush();
  }

  public void flush() throws FinishedException {
    _printWriter.flush();
    checkForLineBreak();
  }

  private void checkForLineBreak() throws FinishedException {
    if (_line >= _lineLimit) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      try {
        _printWriter.print(HIT_ANY_KEY_TO_CONTINUE);
        _printWriter.flush();
        do {
          int read = reader.read();
          if (read != 'q') {
            break;
          } else {
            _finished = true;
            break;
          }
        } while (true);
        _printWriter.print('\r');
        for (int i = 0; i < HIT_ANY_KEY_TO_CONTINUE.length(); i++) {
          _printWriter.print(' ');
        }
        _printWriter.print('\r');
        _printWriter.flush();
        if (_finished) {
          throw new FinishedException();
        }
        _line = 0;
      } catch (IOException e) {
        if (Main.debug) {
          e.printStackTrace();
        }
      }
    }
  }

  public int getLineLimit() {
    return _lineLimit;
  }

  public void setLineLimit(int lineLimit) {
    _lineLimit = lineLimit;
  }

  public boolean isFinished() {
    return _finished;
  }

  public void setFinished(boolean finished) {
    _finished = finished;
  }
}
