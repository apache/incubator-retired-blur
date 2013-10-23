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
package org.apache.blur.lucene.codec;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.Directory;

public class Blur021LiveDocsFormat extends Lucene40LiveDocsFormat {

  private static final String SEP = "_";
  private static final String FILTER = ".filter";
  private static final String SAMPLE = ".sample";

  @Override
  public void files(SegmentInfoPerCommit info, Collection<String> files) throws IOException {
    super.files(info, files);
    Directory dir = info.info.dir;
    String[] listAll = dir.listAll();
    for (String file : listAll) {
      if (file.endsWith(FILTER)) {
        if (file.startsWith(info.info.name + SEP) && file.endsWith(FILTER)) {
          files.add(file);
        }
      } else if (file.equals(info.info.name + SAMPLE)) {
        files.add(file);
      }
    }
  }

}
