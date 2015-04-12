package org.apache.blur.mapreduce.lib;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.mapreduce.Counter;

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

public class NullCounter {

  public static Counter getNullCounter() {
    try {
      // Try hadoop 2 version
      // Class<?> counterClass =
      // Class.forName("org.apache.hadoop.mapreduce.Counter");
      // if (counterClass.isInterface()) {
      // return (Counter) Proxy.newProxyInstance(Counter.class.getClassLoader(),
      // new Class[] { Counter.class },
      // handler);
      // }
      Class<?> clazz1 = Class.forName("org.apache.hadoop.mapreduce.counters.GenericCounter");
      return (Counter) clazz1.newInstance();
    } catch (ClassNotFoundException e1) {
      // Try hadoop 1 version
      try {
        Class<?> clazz2 = Class.forName("org.apache.hadoop.mapreduce.Counter");
        Constructor<?> constructor = clazz2.getDeclaredConstructor(new Class[] {});
        constructor.setAccessible(true);
        return (Counter) constructor.newInstance(new Object[] {});
      } catch (ClassNotFoundException e2) {
        throw new RuntimeException(e2);
      } catch (NoSuchMethodException e2) {
        throw new RuntimeException(e2);
      } catch (SecurityException e2) {
        throw new RuntimeException(e2);
      } catch (InstantiationException e2) {
        throw new RuntimeException(e2);
      } catch (IllegalAccessException e2) {
        throw new RuntimeException(e2);
      } catch (IllegalArgumentException e2) {
        throw new RuntimeException(e2);
      } catch (InvocationTargetException e2) {
        throw new RuntimeException(e2);
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

}
