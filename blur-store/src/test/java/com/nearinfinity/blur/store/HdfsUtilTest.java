package com.nearinfinity.blur.store;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

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
