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

package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.manager.writer.BlurIndexWriter;

public class LocalIndexServer implements IndexServer {
    
    private final static Log LOG = LogFactory.getLog(LocalIndexServer.class);
    
    private Map<String,Map<String, BlurIndex>> readersMap = new ConcurrentHashMap<String, Map<String,BlurIndex>>();
    private File localDir;
    
    public LocalIndexServer(File file) {
        this.localDir = file;
        this.localDir.mkdirs();
    }

    @Override
    public BlurAnalyzer getAnalyzer(String table) {
        return new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30),"");
    }

    @Override
    public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        Map<String, BlurIndex> tableMap = readersMap.get(table);
        if (tableMap == null) {
            tableMap = openFromDisk(table);
            readersMap.put(table, tableMap);
        }
        return tableMap;
    }

    @Override
    public Similarity getSimilarity(String table) {
        return new FairSimilarity();
    }
    
    @Override
    public void close() {
        for (String table : readersMap.keySet()) {
            close(readersMap.get(table));
        }
    }
    
    private void close(Map<String, BlurIndex> map) {
        for (BlurIndex index : map.values()) {
            try {
                index.close();
            } catch (Exception e) {
                LOG.error("Error while trying to close index.",e);
            }
        }
    }

    private Map<String, BlurIndex> openFromDisk(String table) throws IOException {
        File tableFile = new File(localDir,table);
        if (tableFile.isDirectory()) {
            Map<String, BlurIndex> shards = new ConcurrentHashMap<String, BlurIndex>();
            for (File f : tableFile.listFiles()) {
                if (f.isDirectory()) {
//                    Directory directory = FSDirectory.open(f);
                    MMapDirectory directory = new MMapDirectory(f);
                    if (!IndexReader.indexExists(directory)) {
                        new IndexWriter(directory, new KeywordAnalyzer(), MaxFieldLength.UNLIMITED).close();
                    }
                    String shardName = f.getName();
                    shards.put(shardName,openIndex(table,directory));
                }
            }
            return shards;
        }
        throw new IOException("Table [" + table + "] not found.");
    }

    private BlurIndex openIndex(String table, Directory dir) throws CorruptIndexException, IOException {
        BlurIndexWriter writer = new BlurIndexWriter();
        writer.setDirectory(dir);
        writer.setAnalyzer(getAnalyzer(table));
        writer.init();
        return writer;
    }

    @Override
    public TABLE_STATUS getTableStatus(String table) {
        return TABLE_STATUS.ENABLED;
    }

    @Override
    public List<String> getControllerServerList() {
        return Arrays.asList("localhost:40010");
    }

    @Override
    public List<String> getShardServerList() {
        return Arrays.asList("localhost:40020");
    }

    @Override
    public List<String> getTableList() {
        return new ArrayList<String>(readersMap.keySet());
    }

    @Override
    public List<String> getShardList(String table) {
        try {
            List<String> result = new ArrayList<String>();
            File tableFile = new File(localDir,table);
            if (tableFile.isDirectory()) {
                for (File f : tableFile.listFiles()) {
                    if (f.isDirectory()) {
                        result.add(f.getName());
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getOfflineShardServers() {
        return new ArrayList<String>();
    }
    
    @Override
    public List<String> getOnlineShardServers() {
        return getShardServerList();
    }

    @Override
    public String getNodeName() {
        return "localhost";
    }
}
