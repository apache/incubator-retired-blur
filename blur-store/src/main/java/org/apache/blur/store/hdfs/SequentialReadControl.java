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
package org.apache.blur.store.hdfs;

import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_SKIP_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_MERGE_READ_SEQUENTIAL_SKIP_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_MERGE_READ_SEQUENTIAL_THRESHOLD;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.utils.BlurConstants;

public class SequentialReadControl implements Cloneable {

  private final BlurConfiguration _configuration;

  private long _sequentialReadThreshold;
  private long _sequentialReadSkipThreshold;
  private int _sequentialReadDetectorCounter = 0;
  private boolean _sequentialReadAllowed = true;
  private boolean _sequentialRead;

  public SequentialReadControl(BlurConfiguration configuration) {
    _configuration = configuration;
    setup(_configuration, this);
  }

  @Override
  public SequentialReadControl clone() {
    try {
      SequentialReadControl control = (SequentialReadControl) super.clone();
      setup(_configuration, control);
      return control;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setup(BlurConfiguration configuration, SequentialReadControl control) {
    if (Thread.currentThread().getName().startsWith(BlurConstants.SHARED_MERGE_SCHEDULER_PREFIX)) {
      control._sequentialReadThreshold = configuration.getLong(BLUR_SHARD_MERGE_READ_SEQUENTIAL_THRESHOLD, 5L);
      control._sequentialReadSkipThreshold = configuration.getLong(BLUR_SHARD_MERGE_READ_SEQUENTIAL_SKIP_THRESHOLD,
          128L * 1024L);
    } else {
      control._sequentialReadThreshold = configuration.getLong(BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_THRESHOLD, 25L);
      control._sequentialReadSkipThreshold = configuration.getLong(BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_SKIP_THRESHOLD,
          32L * 1024L);
    }
  }

  public boolean isSequentialReadAllowed() {
    return _sequentialReadAllowed;
  }

  public void incrReadDetector() {
    _sequentialReadDetectorCounter++;
  }

  public boolean switchToSequentialRead() {
    if (_sequentialReadDetectorCounter > _sequentialReadThreshold && !_sequentialRead) {
      return true;
    }
    return false;
  }

  public boolean shouldSkipInput(long filePointer, long prevFilePointer) {
    return filePointer - prevFilePointer <= _sequentialReadSkipThreshold;
  }

  public boolean isEnabled() {
    return _sequentialRead;
  }

  public void setEnabled(boolean sequentialRead) {
    _sequentialRead = sequentialRead;
  }

}
