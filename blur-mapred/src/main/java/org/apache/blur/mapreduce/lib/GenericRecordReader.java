/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.mapreduce.lib;

import java.io.IOException;
import java.util.Set;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.codec.Blur024Codec;
import org.apache.blur.mapreduce.lib.BlurInputFormat.BlurInputSplit;
import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;

public class GenericRecordReader {

  private static final String LASTMOD = ".lastmod";

  private static final Log LOG = LogFactory.getLog(GenericRecordReader.class);

  private boolean _setup;
  private Text _rowId;
  private TableBlurRecord _tableBlurRecord;
  private Bits _liveDocs;
  private StoredFieldsReader _fieldsReader;
  private Directory _directory;
  private Directory _readingDirectory;

  private int _docId = -1;
  private int _maxDoc;
  private Text _table;

  public void initialize(BlurInputSplit blurInputSplit, Configuration configuration) throws IOException {
    if (_setup) {
      return;
    }
    _setup = true;
    _table = blurInputSplit.getTable();
    Path localCachePath = BlurInputFormat.getLocalCachePath(configuration);
    LOG.info("Local cache path [{0}]", localCachePath);
    _directory = BlurInputFormat.getDirectory(configuration, _table.toString(), blurInputSplit.getDir());

    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(_directory, blurInputSplit.getSegmentsName());
    SegmentInfoPerCommit commit = findSegmentInfoPerCommit(segmentInfos, blurInputSplit);

    SegmentInfo segmentInfo = commit.info;
    if (localCachePath != null) {
      _readingDirectory = copyFilesLocally(configuration, _directory, _table.toString(), blurInputSplit.getDir(),
          localCachePath, segmentInfo.files());
    } else {
      _readingDirectory = _directory;
    }

    Blur024Codec blur024Codec = new Blur024Codec();
    IOContext iocontext = IOContext.READ;

    String segmentName = segmentInfo.name;
    FieldInfos fieldInfos = blur024Codec.fieldInfosFormat().getFieldInfosReader()
        .read(_readingDirectory, segmentName, iocontext);
    if (commit.getDelCount() > 0) {
      _liveDocs = blur024Codec.liveDocsFormat().readLiveDocs(_readingDirectory, commit, iocontext);
    }
    _fieldsReader = blur024Codec.storedFieldsFormat().fieldsReader(_readingDirectory, segmentInfo, fieldInfos,
        iocontext);

    _maxDoc = commit.info.getDocCount();
  }

  private static Directory copyFilesLocally(Configuration configuration, Directory dir, String table, Path shardDir,
      Path localCachePath, Set<String> files) throws IOException {
    LOG.info("Copying files need to local cache for faster reads [{0}].", shardDir);
    Path localShardPath = new Path(new Path(localCachePath, table), shardDir.getName());
    HdfsDirectory localDir = new HdfsDirectory(configuration, localShardPath);
    for (String name : files) {
      if (!isValidFileToCache(name)) {
        continue;
      }
      LOG.info("Valid file for local copy [{0}].", name);
      if (!isValid(localDir, dir, name)) {
        LastModified lastModified = (LastModified) dir;
        long fileModified = lastModified.getFileModified(name);

        IndexInput input = dir.openInput(name, IOContext.READONCE);
        IndexOutput output = localDir.createOutput(name, IOContext.READONCE);
        output.copyBytes(input, input.length());
        output.close();
        IndexOutput lastMod = localDir.createOutput(name + LASTMOD, IOContext.DEFAULT);
        lastMod.writeLong(fileModified);
        lastMod.close();
      }
    }
    return localDir;
  }

  private static boolean isValidFileToCache(String name) {
    if (name.endsWith(".fdt")) {
      return true;
    } else if (name.endsWith(".fdx")) {
      return true;
    } else if (name.endsWith(".del")) {
      return true;
    } else if (name.endsWith(".fnm")) {
      return true;
    } else {
      return false;
    }
  }

  private static boolean isValid(HdfsDirectory localDir, Directory remoteDir, String name) throws IOException {
    LastModified lastModified = (LastModified) remoteDir;
    long fileModified = lastModified.getFileModified(name);
    long fileLength = remoteDir.fileLength(name);

    if (localDir.fileExists(name)) {
      LOG.info("Cache file exists [{0}]", name);
      if (localDir.fileLength(name) == fileLength) {
        LOG.info("Cache file length matches [{0}]", name);
        if (localDir.fileExists(name + LASTMOD)) {
          LOG.info("Cache file last mod file exists [{0}]", name);
          IndexInput input = localDir.openInput(name + LASTMOD, IOContext.DEFAULT);
          long lastMod = input.readLong();
          if (lastMod == fileModified) {
            LOG.info("Cache file last mod matches [{0}]", name);
            return true;
          } else {
            LOG.info("Cache file last mod does not match [{0}]", name);
          }
        } else {
          LOG.info("Cache file last mod file does not exist [{0}]", name);
        }
      } else {
        LOG.info("Cache file length does not match [{0}]", name);
      }
    } else {
      LOG.info("Cache file does not exist [{0}]", name);
    }
    return false;
  }

  private SegmentInfoPerCommit findSegmentInfoPerCommit(SegmentInfos segmentInfos, BlurInputSplit blurInputSplit)
      throws IOException {
    String segmentInfoName = blurInputSplit.getSegmentInfoName();
    for (SegmentInfoPerCommit commit : segmentInfos) {
      if (commit.info.name.equals(segmentInfoName)) {
        return commit;
      }
    }
    throw new IOException("SegmentInfoPerCommit of [" + segmentInfoName + "] not found.");
  }

  public boolean nextKeyValue() throws IOException {
    if (_docId >= _maxDoc) {
      return false;
    }
    while (true) {
      _docId++;
      if (_docId >= _maxDoc) {
        return false;
      }
      if (_liveDocs == null) {
        fetchBlurRecord();
        return true;
      } else if (_liveDocs.get(_docId)) {
        fetchBlurRecord();
        return true;
      }
    }
  }

  private void fetchBlurRecord() throws IOException {
    DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
    _fieldsReader.visitDocument(_docId, visitor);
    BlurRecord blurRecord = new BlurRecord();
    String rowId = RowDocumentUtil.readRecord(visitor.getDocument(), blurRecord);
    blurRecord.setRowId(rowId);
    _rowId = new Text(rowId);
    _tableBlurRecord = new TableBlurRecord(_table, blurRecord);
  }

  public Text getCurrentKey() throws IOException {
    return _rowId;
  }

  public TableBlurRecord getCurrentValue() throws IOException {
    return _tableBlurRecord;
  }

  public float getProgress() throws IOException {
    return (float) _docId / (float) _maxDoc;
  }

  public void close() throws IOException {
    _fieldsReader.close();
    _directory.close();
    if (_readingDirectory != _directory) {
      _readingDirectory.close();
    }
  }
}
