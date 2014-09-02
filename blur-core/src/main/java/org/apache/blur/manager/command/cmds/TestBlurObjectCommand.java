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
package org.apache.blur.manager.command.cmds;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.manager.command.BlurObject;
import org.apache.blur.manager.command.ClusterCommand;
import org.apache.blur.manager.command.ClusterContext;
import org.apache.blur.manager.command.IndexContext;
import org.apache.blur.manager.command.IndexReadCombiningCommand;
import org.apache.blur.manager.command.Server;
import org.apache.blur.manager.command.Shard;

@SuppressWarnings("serial")
public class TestBlurObjectCommand extends BaseCommand implements IndexReadCombiningCommand<BlurObject, BlurObject>,
    ClusterCommand<BlurObject> {

  @Override
  public BlurObject execute(IndexContext context) throws IOException {
    BlurObject blurObject = new BlurObject();
    blurObject.accumulate("docCount", context.getIndexReader().numDocs());
    return blurObject;
  }

  @Override
  public BlurObject combine(Map<Shard, BlurObject> results) throws IOException {
    BlurObject blurObject = new BlurObject();
    long total = 0;
    for (Entry<Shard, BlurObject> e : results.entrySet()) {
      total += e.getValue().getInteger("docCount");
    }
    blurObject.put("docCount", total);
    return blurObject;
  }

  @Override
  public BlurObject clusterExecute(ClusterContext context) throws IOException {
    BlurObject blurObject = new BlurObject();
    Map<Server, BlurObject> results = context.readServers(null, TestBlurObjectCommand.class);
    long total = 0;
    for (Entry<Server, BlurObject> e : results.entrySet()) {
      BlurObject value = e.getValue();
      Long count = value.getLong("docCount");
      total += count;
    }
    blurObject.put("docCount", total);
    return blurObject;
  }

  @Override
  public String getName() {
    return "testBlurObject";
  }

}
