package org.apache.blur.zookeeper;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.CachedMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This is an simple implementation of a set-once map of string-to-string that
 * is backed by ZooKeeper. Meaning that once the value is set a single time it
 * cannot be set to a different value. The clear cache method is called when the
 * internal cache is to be cleared and re-read from ZooKeeper. <br>
 * <br>
 * Usage:<br>
 * <br>
 * ZkCachedMap map = new ZkCachedMap(zooKeeper, path);<br>
 * String key = "key";<br>
 * String newValue = "value";<br>
 * String value = map.get(key);<br>
 * if (value == null) {<br>
 * &nbsp;&nbsp;if (map.putIfMissing(key, newValue)) {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println("Yay! My value was taken.");<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;value = newValue;<br>
 * &nbsp;&nbsp;} else {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println("Boo! Someone beat me to it.");<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;value = map.get(key);<br>
 * &nbsp;&nbsp;}<br>
 * }<br>
 * System.out.println("key [" + key + "] value [" + value + "]");<br>
 * 
 */
public class ZkCachedMap extends CachedMap {

  private static final String SEP = "-";

  private final Map<String, String> cache = new ConcurrentHashMap<String, String>();
  private final ZooKeeper zooKeeper;
  private final String basePath;

  public ZkCachedMap(ZooKeeper zooKeeper, String basePath) {
    this.zooKeeper = zooKeeper;
    this.basePath = basePath;
  }

  @Override
  public void clearCache() {
    cache.clear();
  }

  /**
   * Checks the in memory map first, then fetches from ZooKeeper.
   * 
   * @param key
   *          the key.
   * @return the value, null if it does not exist.
   * @exception IOException
   *              if there is an io error.
   */
  @Override
  public String get(String key) throws IOException {
    String value = cache.get(key);
    if (value != null) {
      return value;
    }
    return getFromZooKeeper(key);
  }

  /**
   * Checks the in memory map first, if it exists then return true. If missing
   * then check ZooKeeper.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   * @return boolean, true if the put was successful, false if a value already
   *         exists.
   * @exception IOException
   *              if there is an io error.
   */
  @Override
  public boolean putIfMissing(String key, String value) throws IOException {
    String existingValue = cache.get(key);
    if (existingValue != null) {
      return false;
    }
    return putIfMissingFromZooKeeper(key, value);
  }

  private String getFromZooKeeper(String key) throws IOException {
    try {
      List<String> keys = new ArrayList<String>(zooKeeper.getChildren(basePath, false));
      Collections.sort(keys);
      for (String k : keys) {
        String realKey = getRealKey(k);
        if (realKey.equals(key)) {
          String path = getPath(k);
          byte[] data = getValue(path);
          if (data == null) {
            return null;
          }
          String value = new String(data);
          cache.put(key, value);
          return value;
        }
      }
      return null;
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private byte[] getValue(String path) throws KeeperException, InterruptedException {
    Stat stat = zooKeeper.exists(path, false);
    if (stat == null) {
      return null;
    }
    byte[] data = zooKeeper.getData(path, false, stat);
    if (data == null) {
      return null;
    }
    return data;
  }

  private boolean putIfMissingFromZooKeeper(String key, String value) throws IOException {
    try {
      String path = getPath(key);
      String newPath = zooKeeper.create(path + SEP, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
      String keyWithSeq = getKeyWithSeq(newPath);
      List<String> keys = new ArrayList<String>(zooKeeper.getChildren(basePath, false));
      Collections.sort(keys);
      for (String k : keys) {
        String realKey = getRealKey(k);
        if (realKey.equals(key)) {
          if (keyWithSeq.equals(k)) {
            // got the lock
            cache.put(key, value);
            return true;
          } else {
            // remove duplicate key
            zooKeeper.delete(newPath, -1);
            return false;
          }
        }
      }
      return false;
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private String getKeyWithSeq(String newPath) {
    int lastIndexOf = newPath.lastIndexOf('/');
    if (lastIndexOf < 0) {
      throw new RuntimeException("Path [" + newPath + "] does not contain [/]");
    }
    return newPath.substring(lastIndexOf + 1);
  }

  private String getRealKey(String keyWithSeq) {
    int lastIndexOf = keyWithSeq.lastIndexOf(SEP);
    if (lastIndexOf < 0) {
      throw new RuntimeException("Key [" + keyWithSeq + "] does not contain [" + SEP + "]");
    }
    return keyWithSeq.substring(0, lastIndexOf);
  }

  private String getPath(String key) {
    return basePath + "/" + key;
  }

  @Override
  public Map<String, String> fetchAllFromSource() throws IOException {
    try {
      Map<String, String> result = new HashMap<String, String>();
      List<String> keys = new ArrayList<String>(zooKeeper.getChildren(basePath, false));
      Collections.sort(keys);
      for (String k : keys) {
        String realKey = getRealKey(k);
        String path = getPath(k);
        byte[] value = getValue(path);
        if (value != null) {
          result.put(realKey, new String(value));
        }
      }
      return result;
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }

}
