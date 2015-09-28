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
package org.apache.blur.command.stream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.blur.command.IndexContext;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.commons.io.IOUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class StreamProcessor {

  private static final Log LOG = LogFactory.getLog(StreamProcessor.class);

  private final IndexServer _indexServer;
  private final Map<String, ClassLoader> _classLoaderMap;
  private final File _tmpFile;

  public StreamProcessor(IndexServer indexServer, File tmpFile) {
    _indexServer = indexServer;
    _classLoaderMap = CacheBuilder.newBuilder().concurrencyLevel(4).maximumSize(128)
        .expireAfterAccess(45, TimeUnit.SECONDS).removalListener(new RemovalListener<String, ClassLoader>() {
          @Override
          public void onRemoval(RemovalNotification<String, ClassLoader> notification) {
            String key = notification.getKey();
            LOG.info("Unloading classLoaderId [{0}]", key);
            File file = new File(_tmpFile, key);
            if (!rmr(file)) {
              LOG.error("Could not remove file [{0}]", file);
            }
          }
        }).build().asMap();

    _tmpFile = tmpFile;
  }

  protected boolean rmr(File file) {
    boolean success = true;
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File f : file.listFiles()) {
          if (!rmr(f)) {
            success = false;
          }
        }
      }
      if (!file.delete()) {
        success = false;
      }
    }
    return success;
  }

  public StreamIndexContext getIndexContext(final String table, final String shard) throws IOException {
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    if (indexes == null) {
      throw new IOException("Table [" + table + "] is not being served by this server.");
    }
    BlurIndex blurIndex = indexes.get(shard);
    if (blurIndex == null) {
      throw new IOException("Shard [" + shard + "] for table [" + table + "] is not being served by this server.");
    }
    return new StreamIndexContext(blurIndex);
  }

  public <T> void execute(StreamFunction<T> function, OutputStream outputStream, IndexContext indexContext)
      throws IOException {
    final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    StreamWriter<T> writer = getWriter(objectOutputStream);
    try {
      function.call(indexContext, writer);
    } catch (Exception e) {
      LOG.error("Unknown error.", e);
      objectOutputStream.writeObject(new StreamError(e));
    } finally {
      objectOutputStream.writeObject(new StreamComplete());
      objectOutputStream.close();
    }
  }

  private <T> StreamWriter<T> getWriter(final ObjectOutputStream objectOutputStream) {
    final WriteLock writeLock = new ReentrantReadWriteLock(true).writeLock();
    return new StreamWriter<T>() {
      @Override
      public void write(T obj) throws IOException {
        writeLock.lock();
        try {
          objectOutputStream.writeObject(obj);
        } finally {
          writeLock.unlock();
        }
      }

      @Override
      public void write(Iterable<T> it) throws IOException {
        writeLock.lock();
        try {
          for (T t : it) {
            objectOutputStream.writeObject(t);
          }
        } finally {
          writeLock.unlock();
        }
      }
    };
  }

  public StreamFunction<?> getStreamFunction(String classLoaderId, InputStream inputStream) throws IOException {
    final ClassLoader classLoader = getClassLoader(classLoaderId);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream) {
      @Override
      protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        return classLoader.loadClass(desc.getName());
      }
    };
    try {
      return (StreamFunction<?>) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      objectInputStream.close();
    }
  }

  private ClassLoader getClassLoader(String classLoaderId) throws IOException {
    ClassLoader classLoader = _classLoaderMap.get(classLoaderId);
    if (classLoader == null) {
      throw new IOException("ClassLoaderId [" + classLoaderId + "] not found.");
    }
    return classLoader;
  }

  public synchronized void loadClassLoader(String classLoaderId, DataInputStream inputStream) throws IOException {
    if (_classLoaderMap.containsKey(classLoaderId)) {
      // read input and discard
      int length = inputStream.readInt();
      byte[] buf = new byte[length];
      inputStream.readFully(buf);
      return;
    }

    LOG.info("Class Loader [{0}] Starting", classLoaderId);
    File copyJarsLocally = copyJarsLocally(classLoaderId, inputStream);
    List<URL> urls = new ArrayList<URL>();
    for (File f : copyJarsLocally.listFiles()) {
      URL url = f.toURI().toURL();
      LOG.info("Class Loader [{0}] Loading [{1}]", classLoaderId, url);
      urls.add(url);
    }
    URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[] {}));
    _classLoaderMap.put(classLoaderId, classLoader);
    LOG.info("Class Loader [{0}] Complete", classLoaderId);
  }

  private File copyJarsLocally(String classLoaderId, DataInputStream inputStream) throws IOException,
      FileNotFoundException {
    int length = inputStream.readInt();
    byte[] buf = new byte[length];
    inputStream.readFully(buf);
    ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(buf));
    try {
      ZipEntry zipEntry;
      File dir = new File(_tmpFile, classLoaderId);
      dir.mkdirs();
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.isDirectory()) {
          throw new IOException("Dirs in delivery zip are not supported.");
        }
        String name = zipEntry.getName();
        File file = new File(dir, name);
        FileOutputStream output = new FileOutputStream(file);
        IOUtils.copy(zipInputStream, output);
        output.close();
      }
      return dir;
    } finally {
      zipInputStream.close();
    }
  }

  public boolean isClassLoaderLoaded(String id) {
    return _classLoaderMap.containsKey(id);
  }
}
