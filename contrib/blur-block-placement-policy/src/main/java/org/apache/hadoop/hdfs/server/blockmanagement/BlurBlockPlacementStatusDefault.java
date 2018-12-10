package org.apache.hadoop.hdfs.server.blockmanagement;
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

import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;

public class BlurBlockPlacementStatusDefault implements BlockPlacementStatus {

  private final BlockPlacementStatus _original;
  private final String _shardServer;
  private final boolean _origPlacementPolicySatisfied;

  public BlurBlockPlacementStatusDefault(BlockPlacementStatus original, String shardServer) {
    _original = original;
    _shardServer = shardServer;
    if (_original != null) {
      _origPlacementPolicySatisfied = _original.isPlacementPolicySatisfied();
    } else {
      _origPlacementPolicySatisfied = true;
    }
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    if (_shardServer == null) {
      return _origPlacementPolicySatisfied;
    } else {
      return false;
    }
  }

  @Override
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }
    String message = "Block should located on shard server [" + _shardServer + "] for best performance";
    if (_origPlacementPolicySatisfied) {
      return message + ".";
    } else {
      return message + " AND " + _original.getErrorDescription() + ".";
    }
  }

}
