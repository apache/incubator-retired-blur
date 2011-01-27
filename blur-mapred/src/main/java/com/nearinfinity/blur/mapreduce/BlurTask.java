package com.nearinfinity.blur.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.lucene.index.IndexCommit;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.store.LocalFileCache;

public class BlurTask {

    public static final String SHARD_PREFIX = "shard-";
    public static final String EMPTY = "EMPTY";
    public static final String BLUR_COMMIT = "blur.commit";
    public static final String BLUR_TABLE_NAME = "blur.table.name";
    public static final String BLUR_COMMIT_POINT_NEW = "blur.commit.point.new";
    public static final String BLUR_COMMIT_POINT_TO_OPEN = "blur.commit.point.to.open";
    public static final String BLUR_BASE_PATH = "blur.base.path";
    public static final String BLUR_ANALYZER_JSON = "blur.analyzer.json";
    public static final String BLUR_RAM_BUFFER_SIZE = "blur.ram.buffer.size";
    public static final String MAPRED_LOCAL_DIR = "mapred.local.dir";
    public static final String BLUR_MAPPER_MAX_RECORD_COUNT = "blur.mapper.max.record.count";
    
    private Configuration configuration;
    private String shardName;

    public BlurTask(TaskAttemptContext context) {
        this(context.getConfiguration());
        //need to figure out shard name
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        int id = taskAttemptID.getTaskID().getId();
        shardName = SHARD_PREFIX + buffer(id,8);
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

    public LocalFileCache getLocalFileCache() {
        LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(getFiles(configuration.get(MAPRED_LOCAL_DIR)));
        localFileCache.open();
        return localFileCache;
    }

    public Map<String, String> getCommitUserData() {
        String commitPoint = getNewCommitPoint();
        Map<String, String> userData = new HashMap<String,String>();
        userData.put(BLUR_COMMIT, commitPoint);
        return userData;
    }

    public Map<String, String> getEmptyCommitUserData() {
        Map<String, String> userData = new HashMap<String,String>();
        userData.put(BLUR_COMMIT, EMPTY);
        return userData;
    }
    
    public IndexCommit getIndexCommitPointNameToOpen(Collection<IndexCommit> listCommits) throws IOException {
        String commitPointToOpen = getCommitPointToOpen();
        for (IndexCommit commit : listCommits) {
            Map<String, String> userData = commit.getUserData();
            String commitStr = userData.get(BLUR_COMMIT);
            if (commitPointToOpen.equals(commitStr)) {
                return commit;
            }
        }
        throw new IOException("Commit point [" + commitPointToOpen + "] not found.");
    }

    public BlurAnalyzer getAnalyzer() {
        try {
            return BlurAnalyzer.create(configuration.get(BLUR_ANALYZER_JSON));
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

    public void setBlurAnalyzerStr(String blurAnalyzerStr) {
        configuration.set(BLUR_ANALYZER_JSON, blurAnalyzerStr);
    }
    
    public String getCommitPointToOpen() {
        return configuration.get(BLUR_COMMIT_POINT_TO_OPEN);
    }

    public void setCommitPointToOpen(String commitPointToOpen) {
        configuration.set(BLUR_COMMIT_POINT_TO_OPEN, commitPointToOpen);
    }

    public String getNewCommitPoint() {
        return configuration.get(BLUR_COMMIT_POINT_NEW);
    }

    public void setNewCommitPoint(String newCommitPoint) {
        configuration.set(BLUR_COMMIT_POINT_NEW, newCommitPoint);
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
    
    private File[] getFiles(String dirs) {
        String[] split = dirs.split(",");
        File[] files = new File[split.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(split[i]);
        }
        return files;
    }
    
    private String buffer(int value, int length) {
        String str = Integer.toString(value);
        while (str.length() < length) {
            str = "0" + str;
        }
        return str;
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

}
