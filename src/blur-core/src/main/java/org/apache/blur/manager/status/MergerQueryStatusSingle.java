package org.apache.blur.manager.status;

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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.ForkJoin.Merger;


public class MergerQueryStatusSingle implements Merger<BlurQueryStatus> {

  private long _timeout;

  public MergerQueryStatusSingle(long timeout) {
    _timeout = timeout;
  }

  @Override
  public BlurQueryStatus merge(BlurExecutorCompletionService<BlurQueryStatus> service) throws BlurException {
    BlurQueryStatus result = null;
    while (service.getRemainingCount() > 0) {
      Future<BlurQueryStatus> future = service.poll(_timeout, TimeUnit.MILLISECONDS, true);
      BlurQueryStatus status = service.getResultThrowException(future);
      if (result == null) {
        result = status;
      } else {
        result = MergerQueryStatus.merge(result, status);
      }
    }
    return result;
  }
}
