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

package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.manager.hits.HitsComparator;
import com.nearinfinity.blur.manager.hits.HitsPeekableIteratorComparator;
import com.nearinfinity.blur.manager.hits.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;


public class BlurConstants {
	
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final String SHARD_PREFIX = "shard-";
	public static final Comparator<? super ColumnFamily> COLUMN_FAMILY_COMPARATOR = new ColumnFamilyComparator();
	public static final Comparator<? super Column> COLUMN_COMPARATOR = new ColumnComparator();
    public static final Comparator<? super PeekableIterator<BlurResult>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new HitsPeekableIteratorComparator();
    public static final Comparator<? super BlurResult> HITS_COMPARATOR = new HitsComparator();

    public static final String PRIME_DOC = "_prime_";
    public static final String PRIME_DOC_VALUE = "true";
    public static final String ROW_ID = "rowid";
    public static final String RECORD_ID = "recordid";
    public static final String SEP = ".";
}
