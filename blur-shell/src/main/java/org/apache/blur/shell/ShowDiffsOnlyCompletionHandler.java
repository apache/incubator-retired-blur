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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import jline.console.ConsoleReader;
import jline.console.CursorBuffer;
import jline.console.completer.CandidateListCompletionHandler;

public class ShowDiffsOnlyCompletionHandler extends CandidateListCompletionHandler {

  private final Method _method;

  public ShowDiffsOnlyCompletionHandler() {
    try {
      _method = CandidateListCompletionHandler.class.getDeclaredMethod("getUnambiguousCompletions",
          new Class[] { List.class });
      _method.setAccessible(true);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean complete(final ConsoleReader reader, final List<CharSequence> candidates, final int pos)
      throws IOException {
    CursorBuffer buf = reader.getCursorBuffer();

    // if there is only one completion, then fill in the buffer
    if (candidates.size() == 1) {
      CharSequence value = candidates.get(0);

      // fail if the only candidate is the same as the current buffer
      if (value.equals(buf.toString())) {
        return false;
      }

      CandidateListCompletionHandler.setBuffer(reader, value, pos);

      return true;
    } else if (candidates.size() > 1) {
      String value = getUnambiguousCompletionsInternal(candidates);
      CandidateListCompletionHandler.setBuffer(reader, value, pos);
    }

    printCandidates(reader, removeLeadingValues(buf.toString(), candidates));

    // redraw the current console buffer
    reader.drawLine();
    return true;
  }

  private Collection<CharSequence> removeLeadingValues(String buf, List<CharSequence> candidates) {
    int index = buf.lastIndexOf(' ');
    if (index < 0) {
      return candidates;
    }
    List<CharSequence> result = new ArrayList<CharSequence>();
    String prefix = buf.substring(0, index);
    for (CharSequence c : candidates) {
      String s = c.toString();
      if (s.startsWith(prefix)) {
        result.add(s.substring(index + 1));
      } else {
        result.add(s);
      }
    }
    return result;
  }

  private String getUnambiguousCompletionsInternal(List<CharSequence> candidates) {
    try {
      return (String) _method.invoke(this, candidates);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }

  }

}