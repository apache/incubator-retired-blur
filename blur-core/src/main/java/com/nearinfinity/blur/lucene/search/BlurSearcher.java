/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.utils.PrimeDocCache.IndexReaderCache;

public class BlurSearcher extends IndexSearcher {

    private IndexReaderCache indexReaderCache;

    public BlurSearcher(Directory path, IndexReaderCache indexReaderCache) throws CorruptIndexException, IOException {
        super(path);
        this.indexReaderCache = indexReaderCache;
    }

    public BlurSearcher(Directory path, boolean readOnly, IndexReaderCache indexReaderCache) throws CorruptIndexException, IOException {
        super(path, readOnly);
        this.indexReaderCache = indexReaderCache;
    }

    public BlurSearcher(IndexReader reader, IndexReader[] subReaders, int[] docStarts, IndexReaderCache indexReaderCache) {
        super(reader, subReaders, docStarts);
        this.indexReaderCache = indexReaderCache;
    }

    public BlurSearcher(IndexReader r, IndexReaderCache indexReaderCache) {
        super(r);
        this.indexReaderCache = indexReaderCache;
    }
    
    public IndexReaderCache getIndexReaderCache() {
        return indexReaderCache;
    }

    
}
