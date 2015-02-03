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

import static org.apache.blur.utils.BlurConstants.BLUR_LUCENE_FST_BYTEARRAY_FACTORY;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public abstract class ByteArrayFactory {

  private static Log LOG = LogFactory.getLog(ByteArrayFactory.class);

  private final static ByteArrayFactory _factory;

  static {
    BlurConfiguration configuration;
    try {
      configuration = new BlurConfiguration();
    } catch (IOException e) {
      LOG.error("Error while trying to open blur config.", e);
      try {
        configuration = new BlurConfiguration(false);
      } catch (IOException ex) {
        throw new RuntimeException();
      }
    }
    ByteArrayFactory factory;
    try {
      String className = configuration
          .get(BLUR_LUCENE_FST_BYTEARRAY_FACTORY, ByteArrayPrimitiveFactory.class.getName());
      Class<?> clazz = Class.forName(className);
      Constructor<?> constructor = clazz.getConstructor(new Class[] { BlurConfiguration.class });
      factory = (ByteArrayFactory) constructor.newInstance(new Object[] { configuration });
    } catch (Exception e) {
      LOG.error("Error while trying create new bytearray factory for lucene bytestore.", e);
      throw new RuntimeException(e);
    }
    _factory = factory;
  }

  public ByteArrayFactory(BlurConfiguration configuration) {

  }

  public abstract ByteArray newByteArray(int size);

  public static ByteArrayFactory getDefaultFactory() {
    return _factory;
  }

}
