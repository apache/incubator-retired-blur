package org.apache.blur.command.example;

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
import java.io.IOException;

import org.apache.blur.command.BlurArray;
import org.apache.blur.command.TermsCommand;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;

public class UsingTermCommand {

  public static void main(String[] args) throws BlurException, TException, IOException {
    TermsCommand command = new TermsCommand();
    command.setTable("test");
    command.setFieldName("fam0.col0");
    BlurArray blurArray = command.run("localhost:40020");
    System.out.println(blurArray);
  }
}
