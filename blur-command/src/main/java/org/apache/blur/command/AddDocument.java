package org.apache.blur.command;

import java.io.IOException;

import org.apache.blur.command.Args;
import org.apache.blur.command.Command;
import org.apache.blur.command.IndexContext;
import org.apache.blur.command.IndexWriteCommand;
import org.apache.blur.command.annotation.Argument;
import org.apache.blur.command.annotation.Arguments;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

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

@SuppressWarnings("serial")
@Arguments({
    @Argument(name = "shard", value = "The shard id that the addDocument command is to be applied."),
    @Argument(name = "doc", value = "Is a map of string key to string values that will be converted"
        + "into a Lucene Document via the FieldManager and added to the index.") })
public class AddDocument extends Command implements IndexWriteCommand<Void> {

  @Override
  public String getName() {
    return "addDoc";
  }

  @Override
  public Void execute(IndexContext context, IndexWriter writer) throws IOException {
    Args args = context.getArgs();
    Document doc = getDoc(args);
    writer.addDocument(doc);
    return null;
  }

  private Document getDoc(Args args) {
    throw new RuntimeException("Not Implemented");
  }

}
