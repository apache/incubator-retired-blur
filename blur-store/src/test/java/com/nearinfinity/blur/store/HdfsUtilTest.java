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

package com.nearinfinity.blur.store;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.nearinfinity.blur.store.cache.HdfsUtil;

public class HdfsUtilTest {

    @Test
    public void testDirNameMethod() {
        assertEquals("table__shard",HdfsUtil.getDirName("table", "shard"));
    }
    
    @Test
    public void testDirNameMethodWithBadTableName() {
        try {
            HdfsUtil.getDirName("table__", "shard");
            fail();
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testDirNameMethodWithBadShardName() {
        try {
            HdfsUtil.getDirName("table", "shar__d");
            fail();
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testGetShard() {
        assertEquals("shard",HdfsUtil.getShard("table__shard"));
    }
    
    @Test
    public void testGetTable() {
        assertEquals("table",HdfsUtil.getTable("table__shard"));
    }
    
    @Test
    public void testGetPath() {
        assertEquals(new Path("/blur/tables/table/shard"),HdfsUtil.getHdfsPath(new Path("/blur/tables"), "table__shard"));
    }
    
}
