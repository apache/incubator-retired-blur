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

package com.nearinfinity.blur.store.replication;

import static org.junit.Assert.*;

import org.junit.Test;

public class LuceneIndexFileComparatorTest {
    
    @Test
    public void testLuceneIndexFileComparator() {
        LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
        assertTrue(comparator.compare("hey.tis", "hey2.tis") < 0);
        assertTrue(comparator.compare("hey.tii", "hey.tis") < 0);
        assertTrue(comparator.compare("hey.tis", "hey.tii") > 0);
    }

}
