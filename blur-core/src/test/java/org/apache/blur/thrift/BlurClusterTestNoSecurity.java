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
package org.apache.blur.thrift;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.blur.user.User;
import org.junit.Test;

public class BlurClusterTestNoSecurity extends BlurClusterTestBase {

  @Override
  protected void setupTableProperties(TableDescriptor tableDescriptor) {
    // do nothing
  }

  @Override
  protected RowMutation mutate(RowMutation rowMutation) {
    return rowMutation;
  }

  @Override
  protected void postTableCreate(TableDescriptor tableDescriptor, Iface client) {

  }

  @Override
  protected User getUser() {
    return null;
  }

  @Override
  protected Map<String, String> getUserAttributes() {
    return new HashMap<String, String>();
  }

}
