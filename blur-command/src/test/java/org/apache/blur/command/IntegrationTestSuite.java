package org.apache.blur.command;

import java.io.IOException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.SuiteCluster;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

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

@RunWith(Suite.class)
@Suite.SuiteClasses({ TermsCommandIntTests.class })
public class IntegrationTestSuite {
  @ClassRule
  public static ExternalResource testCluster = new ExternalResource() {

    @Override
    protected void after() {
      try {
        SuiteCluster.shutdownMiniCluster(IntegrationTestSuite.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void before() throws Throwable {
      try {
        SuiteCluster.setupMiniCluster(IntegrationTestSuite.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };

  @After
  public void tearDown() throws BlurException, TException, IOException {
    Iface client = SuiteCluster.getClient();
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      client.disableTable(table);
      client.removeTable(table, true);
    }
  }
}
