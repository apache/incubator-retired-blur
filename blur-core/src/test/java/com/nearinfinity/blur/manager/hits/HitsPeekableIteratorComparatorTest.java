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

package com.nearinfinity.blur.manager.hits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.BlurConstants;

public class HitsPeekableIteratorComparatorTest {
    
    @Test
    public void testHitsPeekableIteratorComparator() {
        List<PeekableIterator<Hit>> hits = new ArrayList<PeekableIterator<Hit>>();
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("5",5))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("2",2))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("1",1))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>(Arrays.asList(newHit("9",1))).iterator()));
        hits.add(new PeekableIterator<Hit>(new ArrayList<Hit>().iterator()));
        
        Collections.sort(hits,BlurConstants.HITS_PEEKABLE_ITERATOR_COMPARATOR);
        
        for (PeekableIterator<Hit> iterator : hits) {
            System.out.println(iterator.peek());
        }
    }

    private Hit newHit(String id, double score) {
        return new Hit(id, null, null, score, null);
    }

}
