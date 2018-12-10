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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.conf.Configuration;

public class TestServerLookup extends ServerLookup {

  private final Map<String, DatanodeDescriptor[]> _map;
  private final ReadWriteLock _hostmapLock;

  @SuppressWarnings("unchecked")
  public TestServerLookup(Configuration conf, Host2NodesMap host2datanodeMap) {
    super(conf, host2datanodeMap);
    Class<? extends Host2NodesMap> c = host2datanodeMap.getClass();
    try {
      {
        Field declaredField = c.getDeclaredField("map");
        declaredField.setAccessible(true);
        _map = (Map<String, DatanodeDescriptor[]>) declaredField.get(host2datanodeMap);
      }
      {
        Field declaredField = c.getDeclaredField("hostmapLock");
        declaredField.setAccessible(true);
        _hostmapLock = (ReadWriteLock) declaredField.get(host2datanodeMap);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isPathSupported(String srcPath) {
    return srcPath.contains("/shard-");
  }

  @Override
  public String getShardServer(String srcPath) {
    return "host4.foo.com";
  }

  @Override
  public DatanodeDescriptor getDatanodeDescriptor(String shardServer) {
    _hostmapLock.writeLock().lock();
    try {
      Collection<DatanodeDescriptor[]> values = _map.values();
      for (DatanodeDescriptor[] array : values) {
        for (DatanodeDescriptor datanodeDescriptor : array) {
          if (shardServer.equals(datanodeDescriptor.getHostName())) {
            return datanodeDescriptor;
          }
        }
      }
      return null;
    } finally {
      _hostmapLock.writeLock().unlock();
    }
  }

}
