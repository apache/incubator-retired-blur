package ${groupId}.${artifactId};

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BaseClusterTest;
import org.apache.blur.thrift.TableGen;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class MyCommandIntegrationTests extends BaseClusterTest {

  @Test
  public void testTotalTermFreq() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testTotalTermFreq";
    TableGen.define(tableName).cols("test", "col1").addRows(100, 20, "r1", "rec-###", "value").build(getClient());

    TermsCommand command = new TermsCommand();
    command.setTable(tableName);
    command.setFieldName("test.col1");

    List<String> terms = command.run(getClient());
    List<String> list = Lists.newArrayList("value");
    assertEquals(list, terms);
  }

}
