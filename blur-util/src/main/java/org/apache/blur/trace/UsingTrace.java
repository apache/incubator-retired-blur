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
package org.apache.blur.trace;

public class UsingTrace {

  public static void main(String[] args) {

    // Trace.setupTrace("cool");

    Tracer trace = Trace.trace("1");
    long meth1;
    try {
      meth1 = meth1();
    } finally {
      trace.done();
    }
    System.out.println(meth1);

//    Trace.tearDownTrace();
  }

  private static long meth1() {
    Tracer trace = Trace.trace("2");
    try {
      return meth2();
    } finally {
      trace.done();
    }
  }

  private static long meth2() {
    Tracer trace = Trace.trace("3");
    try {
      return meth3();
    } finally {
      trace.done();
    }
  }

  private static long meth3() {
    long t = 0;
    for (long i = 0; i < 10000; i++) {
      t *= i;
    }
    return t;
  }

}
