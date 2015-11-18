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
package org.apache.blur.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;

public class IndexDeletionPolicyReader extends IndexDeletionPolicy {

  private final IndexDeletionPolicy _base;
  private final Set<Long> _gens = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

  public IndexDeletionPolicyReader(IndexDeletionPolicy base) {
    _base = base;
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    _base.onInit(commits);
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    commits = removeCommitsStillInUse(commits);
    _base.onCommit(commits);
  }

  private List<? extends IndexCommit> removeCommitsStillInUse(List<? extends IndexCommit> commits) {
    List<IndexCommit> validForRemoval = new ArrayList<IndexCommit>();
    for (IndexCommit commit : commits) {
      if (!isStillInUse(commit)) {
        validForRemoval.add(commit);
      }
    }
    return validForRemoval;
  }

  public DirectoryReader register(DirectoryReader reader) throws IOException {
    final long generation = reader.getIndexCommit().getGeneration();
    register(generation);
    reader.addReaderClosedListener(new ReaderClosedListener() {
      @Override
      public void onClose(IndexReader reader) {
        unregister(generation);
      }
    });
    return reader;
  }

  private boolean isStillInUse(IndexCommit commit) {
    long generation = commit.getGeneration();
    return _gens.contains(generation);
  }

  public void register(long gen) {
    _gens.add(gen);
  }

  public void unregister(long gen) {
    _gens.remove(gen);
  }

  public int getReaderGenerationCount() {
    return _gens.size();
  }

}
