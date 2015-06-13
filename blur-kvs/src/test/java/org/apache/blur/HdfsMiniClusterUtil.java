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
package org.apache.blur;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class HdfsMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(HdfsMiniClusterUtil.class);

  public static MiniDFSCluster startDfs(Configuration conf, boolean format, String path) {
    String perm;
    Path p = new Path(new File("./target").getAbsolutePath());
    try {
      FileSystem fileSystem = p.getFileSystem(conf);
      FileStatus fileStatus = fileSystem.getFileStatus(p);
      FsPermission permission = fileStatus.getPermission();
      perm = permission.getUserAction().ordinal() + "" + permission.getGroupAction().ordinal() + ""
          + permission.getOtherAction().ordinal();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("dfs.datanode.data.dir.perm=" + perm);
    conf.set("dfs.datanode.data.dir.perm", perm);
    System.setProperty("test.build.data", path);
    try {
      MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, (String[]) null);
      cluster.waitActive();
      return cluster;
    } catch (Exception e) {
      LOG.error("error opening file system", e);
      throw new RuntimeException(e);
    }
  }

  public static void shutdownDfs(MiniDFSCluster cluster) {
    if (cluster != null) {
      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // / Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
        FileSystem.closeAll();
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }

      // This has got to be one of the worst hacks I have ever had to do.
      // This is needed to shutdown 2 thread pools that are not shutdown by
      // themselves.
      ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      Thread[] threads = new Thread[100];
      int enumerate = threadGroup.enumerate(threads);
      for (int i = 0; i < enumerate; i++) {
        Thread thread = threads[i];
        if (thread.getName().startsWith("pool")) {
          if (thread.isAlive()) {
            thread.interrupt();
            LOG.info("Stopping ThreadPoolExecutor [" + thread.getName() + "]");
            Object target = getField(Thread.class, thread, "target");
            if (target != null) {
              ThreadPoolExecutor e = (ThreadPoolExecutor) getField(ThreadPoolExecutor.class, target, "this$0");
              if (e != null) {
                e.shutdownNow();
              }
            }
            try {
              LOG.info("Waiting for thread pool to exit [" + thread.getName() + "]");
              thread.join();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
  }

  private static Object getField(Class<?> c, Object o, String fieldName) {
    try {
      Field field = c.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(o);
    } catch (NoSuchFieldException e) {
      try {
        Field field = o.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(o);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
