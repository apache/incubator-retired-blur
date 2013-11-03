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
package org.apache.blur.lucene.store.refcounter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.WeakIdentityMap;

public abstract class IndexInputReference extends IndexInput {

  protected boolean _isClosed;
  protected boolean _isClone;
  private final WeakIdentityMap<IndexInputReference, Boolean> _clones;

  protected IndexInputReference(String resourceDescription) {
    super(resourceDescription);
    _isClone = false;
    _isClosed = false;
    _clones = WeakIdentityMap.<IndexInputReference, Boolean> newConcurrentHashMap();
  }

  @Override
  public final void close() throws IOException {
    _clones.remove(this);
    if (_isClone) {
      closeClone();
      return;
    }
    for (Iterator<IndexInputReference> it = _clones.keyIterator(); it.hasNext();) {
      closeCloneQuietly(it.next());
    }
    _clones.clear();
    closeBase();
  }

  private static void closeCloneQuietly(IndexInputReference ref) {
    try {
      if (ref != null) {
        ref.closeClone();
      }
    } catch (IOException ioe) {
      // ignore
    }
  }

  @Override
  public IndexInput clone() {
    IndexInputReference clone = (IndexInputReference) super.clone();
    clone._isClone = true;
    _clones.put(clone, Boolean.TRUE);
    return clone;
  }

  @Override
  protected final void finalize() throws Throwable {
    close();
  }

  protected abstract void closeBase() throws IOException;

  protected abstract void closeClone() throws IOException;

}
