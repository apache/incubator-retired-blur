package org.apache.blur.thrift;

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
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;

import org.apache.blur.thrift.generated.BlurException;


public class BException extends BlurException {

  private static final long serialVersionUID = 5846541677293727358L;

  public BException(String message, Throwable t) {
    super(message, toString(t));
  }

  public BException(String message, Object... parameters) {
    this(MessageFormat.format(message.toString(), parameters), (Throwable) null);
  }

  public BException(String message, Throwable t, Object... parameters) {
    this(MessageFormat.format(message.toString(), parameters), t);
  }

  public static String toString(Throwable t) {
    if (t == null) {
      return null;
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(outputStream);
    t.printStackTrace(writer);
    writer.close();
    return new String(outputStream.toByteArray());
  }
}
