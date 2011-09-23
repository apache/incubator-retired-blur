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

package com.nearinfinity.blur.mapreduce;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurTask {
	
	private static final Log log = LogFactory.getLog(BlurTask.class);

    public static final String BLUR_TABLE_NAME = "blur.table.name";
    public static final String BLUR_BASE_PATH = "blur.base.path";
    public static final String BLUR_ANALYZER_JSON = "blur.analyzer.json";
    public static final String BLUR_RAM_BUFFER_SIZE = "blur.ram.buffer.size";
    public static final String BLUR_MAPPER_MAX_RECORD_COUNT = "blur.mapper.max.record.count";
    
    private Configuration configuration;
    private String shardName;

    public BlurTask(TaskAttemptContext context) {
        this(context.getConfiguration());
        //need to figure out shard name
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        int id = taskAttemptID.getTaskID().getId();
        shardName = BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, id);
    }
    
    public BlurTask(Configuration configuration) {
        this.configuration = configuration;
    }

    public String getShardName() {
        return shardName;
    }

    public Path getDirectoryPath() {
        String basePath = getBasePath();
        String tableName = getTableName();
        String shardName = getShardName();
        return new Path(new Path(basePath, tableName), shardName);
    }
    
    public int getNumReducers(int num) {
    	Path shardPath = new Path(getBasePath(), getTableName());
    	try {
			FileSystem fileSystem = FileSystem.get(this.configuration);
			FileStatus[] files = fileSystem.listStatus(shardPath);
			int shardCount = 0;
			for (FileStatus fileStatus : files) {
				if(fileStatus.isDir()) {
					shardCount++;
				}
			}
			if(shardCount == 0) {
				return num;
			}
			if(shardCount != num) {
				log.warn("asked for " + num + " reducers, but existing table " + getTableName() + " has " + shardCount + " shards. Using " + shardCount + " reducers");
			}
			return shardCount;
		} catch (IOException e) {
			throw new RuntimeException("unable to connect to filesystem", e);
		}
    }

    public BlurAnalyzer getAnalyzer() {
        try {
            return BlurAnalyzer.create(new String(Base64.decodeBase64(configuration.get(BLUR_ANALYZER_JSON))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getRamBufferSizeMB() {
        return configuration.getInt(BLUR_RAM_BUFFER_SIZE,256);
    }
    
    public void setRamBufferSizeMB(int ramBufferSizeMB) {
        configuration.setInt(BLUR_RAM_BUFFER_SIZE, ramBufferSizeMB);
    }

    public String getBlurAnalyzerStr() {
        return configuration.get(BLUR_ANALYZER_JSON);
    }

    public void setBlurAnalyzer(BlurAnalyzer blurAnalyzer) {
        configuration.set(BLUR_ANALYZER_JSON, new String(Base64.encodeBase64(blurAnalyzer.toJSON().getBytes())));
    }
    
    public String getTableName() {
        return configuration.get(BLUR_TABLE_NAME);
    }

    public void setTableName(String tableName) {
        configuration.set(BLUR_TABLE_NAME, tableName);
    }
    
    public String getBasePath() {
        return configuration.get(BLUR_BASE_PATH);
    }

    public void setBasePath(String tableName) {
        configuration.set(BLUR_BASE_PATH, tableName);
    }
    
    public long getMaxRecordCount() {
        return configuration.getLong(BLUR_MAPPER_MAX_RECORD_COUNT,-1L);
    }

    public void setMaxRecordCount(long maxRecordCount) {
        configuration.setLong(BLUR_MAPPER_MAX_RECORD_COUNT, maxRecordCount);
    }
    
    public String getCounterGroupName() {
        return "Blur";
    }

    public String getRowCounterName() {
        return "Rows";
    }

    public String getFieldCounterName() {
        return "Fields";
    }

    public String getRecordCounterName() {
        return "Records";
    }

    public String getRowBreakCounterName() {
        return "Row Retries";
    }

    public String getRowFailureCounterName() {
        return "Row Failures";
    }

}
