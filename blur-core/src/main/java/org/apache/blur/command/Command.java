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

import org.apache.blur.command.annotation.Argument;
import org.apache.blur.command.annotation.OptionalArguments;
import org.apache.blur.command.annotation.RequiredArguments;

@RequiredArguments({ @Argument(name = "table", value = "The name of the table to execute the document count command.", type = String.class) })
@OptionalArguments({ @Argument(name = "shard", value = "The shard id to execute the document count command.", type = String.class) })
public abstract class Command<R> implements Cloneable {

  public abstract String getName();

  public abstract R run(Args arguments) throws IOException;

  public abstract R run(Args arguments, String connectionStr) throws IOException;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Command<R> clone() {
    try {
      return (Command) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
