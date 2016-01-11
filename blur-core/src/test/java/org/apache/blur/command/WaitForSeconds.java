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
import java.util.concurrent.TimeUnit;

import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class WaitForSeconds extends IndexReadCommandSingleTable<Boolean> {

  private static final Log LOG = LogFactory.getLog(WaitForSeconds.class);

  public static void main(String[] args) throws IOException {
    WaitForSeconds waitForSecond = new WaitForSeconds();
    waitForSecond.setSeconds(30);
    waitForSecond.setTable("test1");
    waitForSecond.run("localhost:40010");
  }

  @OptionalArgument("The number of seconds to sleep, the default is 30 seconds.")
  private int seconds = 30;

  @Override
  public Boolean execute(IndexContext context) throws IOException, InterruptedException {
    LOG.info(Thread.currentThread().isInterrupted());
    Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
    return true;
  }

  @Override
  public String getName() {
    return "wait";
  }

  public int getSeconds() {
    return seconds;
  }

  public void setSeconds(int seconds) {
    this.seconds = seconds;
  }

}
