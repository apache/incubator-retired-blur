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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;
import com.nearinfinity.blur.store.hdfs.DirectIODirectory;

public class LocalIndexServer extends AbstractIndexServer {

  private final static Log LOG = LogFactory.getLog(LocalIndexServer.class);

  private Map<String, Map<String, BlurIndex>> _readersMap = new ConcurrentHashMap<String, Map<String, BlurIndex>>();
  private File _localDir;
  private BlurIndexCloser _closer;
  private int _blockSize = 65536;
  private CompressionCodec _compression = CompressedFieldDataDirectory.DEFAULT_COMPRESSION;
  private BlurIndexRefresher _refresher;
  private BlurIndexCommiter _commiter;
  private BlurMetrics _metrics;

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
  public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
    Map<String, BlurIndex> tableMap = _readersMap.get(table);
    Set<String> shardsSet;
    if (tableMap == null) {
      shardsSet = getIndexes(table).keySet();
    } else {
      shardsSet = tableMap.keySet();
    }
    return new TreeSet<String>(shardsSet);
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
          MMapDirectory directory = new MMapDirectory(f);
          if (!IndexReader.indexExists(directory)) {
            new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer())).close();
          }
          String shardName = f.getName();
          shards.put(shardName, openIndex(table, directory));
        }
      }
      return shards;
    }
    throw new IOException("Table [" + table + "] not found.");
  }

  private BlurIndex openIndex(String table, Directory dir) throws CorruptIndexException, IOException {
    BlurIndexWriter writer = new BlurIndexWriter();
    writer.setDirectory(DirectIODirectory.wrap(dir));
    writer.setAnalyzer(getAnalyzer(table));
    writer.setCloser(_closer);
    writer.setRefresher(_refresher);
    writer.setCommiter(_commiter);
    writer.setBlurMetrics(_metrics);
    writer.setSimilarity(getSimilarity(table));
    writer.init();
    return writer;
  }

  @Override
  public TABLE_STATUS getTableStatus(String table) {
    return TABLE_STATUS.ENABLED;
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
    } catch (URISyntaxException e) {
      throw new IOException("bad URI", e);
    }
  }

  private long getFolderSize(File file) {
    long size = 0;
    if (file.isDirectory()) {
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

  public void setBlurMetrics(BlurMetrics metrics) {
    _metrics = metrics;
  }
}
