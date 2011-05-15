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

import com.nearinfinity.blur.manager.results.BlurResultComparator;
import com.nearinfinity.blur.manager.results.BlurResultPeekableIteratorComparator;
import com.nearinfinity.blur.manager.results.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;


public class BlurConstants {
	
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final String SHARD_PREFIX = "shard-";
	public static final Comparator<? super ColumnFamily> COLUMN_FAMILY_COMPARATOR = new ColumnFamilyComparator();
	public static final Comparator<? super Column> COLUMN_COMPARATOR = new ColumnComparator();
    public static final Comparator<? super PeekableIterator<BlurResult>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new BlurResultPeekableIteratorComparator();
    public static final Comparator<? super BlurResult> HITS_COMPARATOR = new BlurResultComparator();

    public static final String PRIME_DOC = "_prime_";
    public static final String PRIME_DOC_VALUE = "true";
    public static final String ROW_ID = "rowid";
    public static final String RECORD_ID = "recordid";
    public static final String SUPER = "super";
    public static final String SEP = ".";
    
    
    
    public static final String BLUR_LOCAL_CACHE_PATHES = "blur.local.cache.pathes";
    public static final String BLUR_TABLE_PATH = "blur.table.path";
    public static final String BLUR_ZOOKEEPER_CONNECTION = "blur.zookeeper.connection";
    public static final String BLUR_SHARD_HOSTNAME = "blur.shard.hostname";
    public static final String BLUR_SHARD_BIND_PORT = "blur.shard.bind.port";
    public static final String BLUR_SHARD_BIND_ADDRESS = "blur.shard.bind.address";
    public static final String BLUR_CONTROLLER_HOSTNAME = "blur.controller.hostname";
    public static final String BLUR_CONTROLLER_BIND_PORT = "blur.controller.bind.port";
    public static final String BLUR_CONTROLLER_BIND_ADDRESS = "blur.controller.bind.address";
    public static final String CRAZY = "CRAZY";
}
