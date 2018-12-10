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
package org.apache.blur.command;

import java.io.IOException;

import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.blur.manager.IndexManager;

public class RunSlowForTesting extends IndexReadCommandSingleTable<Boolean> {

  @OptionalArgument("Set run slow flag.")
  private boolean runSlow = true;

  @Override
  public Boolean execute(IndexContext context) throws IOException, InterruptedException {
    IndexManager.DEBUG_RUN_SLOW.set(runSlow);
    return true;
  }

  @Override
  public String getName() {
    return "RunSlowForTesting";
  }

  public boolean isRunSlow() {
    return runSlow;
  }

  public void setRunSlow(boolean runSlow) {
    this.runSlow = runSlow;
  }

}
