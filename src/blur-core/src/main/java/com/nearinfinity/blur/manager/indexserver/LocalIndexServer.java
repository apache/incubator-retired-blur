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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.manager.writer.BlurIndexCloser;
import com.nearinfinity.blur.manager.writer.BlurIndexCommiter;
import com.nearinfinity.blur.manager.writer.BlurIndexRefresher;
import com.nearinfinity.blur.manager.writer.BlurIndexWriter;
import com.nearinfinity.lucene.compressed.CompressedFieldDataDirectory;

public class LocalIndexServer extends AbstractIndexServer {

    private final static Log LOG = LogFactory.getLog(LocalIndexServer.class);

    private Map<String, Map<String, BlurIndex>> _readersMap = new ConcurrentHashMap<String, Map<String, BlurIndex>>();
    private File _localDir;
    private BlurIndexCloser _closer;
    private int _blockSize = 65536;
    private CompressionCodec _compression = CompressedFieldDataDirectory.DEFAULT_COMPRESSION;
    private BlurIndexRefresher _refresher;
    private BlurIndexCommiter _commiter;

    public LocalIndexServer(File file) {
        _localDir = file;
        _localDir.mkdirs();
        _closer = new BlurIndexCloser();
        _closer.init();
    }

    @Override
    public BlurAnalyzer getAnalyzer(String table) {
        return new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30, new HashSet<String>()));
    }

    @Override
    public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        Map<String, BlurIndex> tableMap = _readersMap.get(table);
        if (tableMap == null) {
            tableMap = openFromDisk(table);
            _readersMap.put(table, tableMap);
        }
        return tableMap;
    }

    @Override
    public Similarity getSimilarity(String table) {
        return new FairSimilarity();
    }

    @Override
    public void close() {
        _closer.close();
        for (String table : _readersMap.keySet()) {
            close(_readersMap.get(table));
        }
    }

    private void close(Map<String, BlurIndex> map) {
        for (BlurIndex index : map.values()) {
            try {
                index.close();
            } catch (Exception e) {
                LOG.error("Error while trying to close index.", e);
            }
        }
    }

    private Map<String, BlurIndex> openFromDisk(String table) throws IOException {
        File tableFile = new File(_localDir, table);
        if (tableFile.isDirectory()) {
            Map<String, BlurIndex> shards = new ConcurrentHashMap<String, BlurIndex>();
            for (File f : tableFile.listFiles()) {
                if (f.isDirectory()) {
                    // Directory directory = FSDirectory.open(f);
                    MMapDirectory directory = new MMapDirectory(f);
                    if (!IndexReader.indexExists(directory)) {
                        new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_33, new KeywordAnalyzer())).close();
                    }
                    warmUp(directory);
                    String shardName = f.getName();
                    shards.put(shardName, openIndex(table, directory));
                }
            }
            return shards;
        }
        throw new IOException("Table [" + table + "] not found.");
    }

    private void warmUp(final MMapDirectory directory) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (String name : directory.listAll()) {
                        if (name.endsWith(".fdx")) {
                            continue;
                        }
                        IndexInput input = directory.openInput(name);
                        long length = input.length();
                        long hash = 0;
                        for (long i = 0; i < length; i++) {
                            hash += input.readByte();
                        }
                        input.close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    private BlurIndex openIndex(String table, Directory dir) throws CorruptIndexException, IOException {
        BlurIndexWriter writer = new BlurIndexWriter();
        writer.setDirectory(dir);
        writer.setAnalyzer(getAnalyzer(table));
        writer.setCloser(_closer);
        writer.setRefresher(_refresher);
        writer.setCommiter(_commiter);
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
        return new ArrayList<String>(_readersMap.keySet());
    }

    @Override
    public List<String> getShardList(String table) {
        try {
            List<String> result = new ArrayList<String>();
            File tableFile = new File(_localDir, table);
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

    @Override
    public String getTableUri(String table) {
        return new File(_localDir, table).toURI().toString();
    }

    @Override
    public int getShardCount(String table) {
        return getShardList(table).size();
    }

    @Override
    public int getCompressionBlockSize(String table) {
        return _blockSize;
    }

    @Override
    public CompressionCodec getCompressionCodec(String table) {
        return _compression;
    }

	@Override
	public long getTableSize(String table) throws IOException {
		try {
			File file = new File(new URI(getTableUri(table)));
			return getFolderSize(file);
		} catch(URISyntaxException e){
			throw new IOException("bad URI", e);
		}
	}
	
	private long getFolderSize(File file){
		long size = 0;
		if(file.isDirectory()){
			for (File sub : file.listFiles()) {
				size += getFolderSize(sub);
			}
		} else {
			size += file.length();
		}
		return size;
	}
	
	public void setRefresher(BlurIndexRefresher refresher) {
        _refresher = refresher;
    }

    public void setCommiter(BlurIndexCommiter commiter) {
        _commiter = commiter;
    }
}
