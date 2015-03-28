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
package org.apache.blur.trace.hdfs;

import static org.apache.blur.utils.BlurConstants.BLUR_HDFS_TRACE_PATH;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.blur.BlurConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HdfsTraceStorageTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp"));

  private HdfsTraceStorage _storage;
  private BlurConfiguration configuration;

  @Before
  public void setUp() throws IOException, InterruptedException {
    rmr(TMPDIR);
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    File testDirectory = new File(TMPDIR, "HdfsTraceStorageTest").getAbsoluteFile();
    testDirectory.mkdirs();

    Path directory = new Path(testDirectory.getPath());
    FsPermission dirPermissions = localFS.getFileStatus(directory).getPermission();
    FsAction userAction = dirPermissions.getUserAction();
    FsAction groupAction = dirPermissions.getGroupAction();
    FsAction otherAction = dirPermissions.getOtherAction();

    StringBuilder builder = new StringBuilder();
    builder.append(userAction.ordinal());
    builder.append(groupAction.ordinal());
    builder.append(otherAction.ordinal());
    String dirPermissionNum = builder.toString();
    System.setProperty("dfs.datanode.data.dir.perm", dirPermissionNum);

    configuration = new BlurConfiguration();
    configuration.set(BLUR_HDFS_TRACE_PATH, directory.makeQualified(localFS).toString());
    _storage = new HdfsTraceStorage(configuration);
    _storage.init(new Configuration());
  }

  @After
  public void tearDown() throws Exception {
    _storage.close();
    rmr(new File("./target/HdfsTraceStorageTest"));
  }

  private void rmr(File file) {
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File f : file.listFiles()) {
          rmr(f);
        }
      }
      file.delete();
    }
  }

  @Test
  public void testStorage() throws IOException, JSONException {
    Random random = new Random();
    createTraceData(random);
    createTraceData(random);
    createTraceData(random);
    List<String> traceIds = _storage.getTraceIds();
    assertEquals(3, traceIds.size());

    for (String traceId : traceIds) {
      List<String> requestIds = _storage.getRequestIds(traceId);
      assertEquals(4, requestIds.size());
      for (String requestId : requestIds) {
        String contents = _storage.getRequestContentsJson(traceId, requestId);
        assertEquals("{\"id\":" + requestId + "}", contents);
      }
    }

    _storage.removeTrace(traceIds.get(0));
    assertEquals(2, _storage.getTraceIds().size());
  }

  private void createTraceData(Random random) throws IOException, JSONException {
    String traceId = Long.toString(Math.abs(random.nextLong()));
    Path path = new Path(configuration.get(BLUR_HDFS_TRACE_PATH));
    Path tracePath = new Path(path, traceId);
    Path storePath = new Path(tracePath, traceId + "_" + Long.toString(Math.abs(random.nextLong())));
    _storage.storeJson(storePath, new JSONObject("{\"id\":" + traceId + "}"));
    writeRequest(random, tracePath);
    writeRequest(random, tracePath);
    writeRequest(random, tracePath);
  }

  private void writeRequest(Random random, Path tracePath) throws IOException, JSONException {
    String requestId = Long.toString(random.nextLong());
    Path storePath = new Path(tracePath, requestId + "_" + Long.toString(Math.abs(random.nextLong())));
    System.out.println(storePath);
    _storage.storeJson(storePath, new JSONObject("{\"id\":" + requestId + "}"));
  }

}
