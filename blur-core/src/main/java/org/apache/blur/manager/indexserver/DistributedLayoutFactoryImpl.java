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
package org.apache.blur.manager.indexserver;

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_DISTRIBUTED_LAYOUT_FACTORY_CLASS;

import org.apache.blur.BlurConfiguration;
import org.apache.zookeeper.ZooKeeper;

public class DistributedLayoutFactoryImpl {

  public static DistributedLayoutFactory getDistributedLayoutFactory(BlurConfiguration configuration, String cluster,
      ZooKeeper zooKeeper) {
    String distributedLayoutFactoryClass = configuration.get(BLUR_SHARD_DISTRIBUTED_LAYOUT_FACTORY_CLASS, "");
    if (distributedLayoutFactoryClass.isEmpty()) {
      return new MasterBasedDistributedLayoutFactory(zooKeeper, cluster);
    }
    try {
      Class<?> clazz = Class.forName(distributedLayoutFactoryClass);
      return (DistributedLayoutFactory) clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
