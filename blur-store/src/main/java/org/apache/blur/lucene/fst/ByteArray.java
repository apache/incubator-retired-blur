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
package org.apache.blur.lucene.fst;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public abstract class ByteArray {

  public abstract int length();

  public abstract void put(int position, byte b);

  public abstract byte get(int position);

  public abstract void put(int position, byte[] b, int offset, int len);

  public abstract void get(int position, byte[] b, int offset, int len);

  public abstract void readBytes(DataInput in, int offset, int length) throws IOException;

  public abstract void writeBytes(DataOutput out, int offset, int length) throws IOException;

  public abstract void copy(int position, ByteArray dest, int destOffset, int len);

}
