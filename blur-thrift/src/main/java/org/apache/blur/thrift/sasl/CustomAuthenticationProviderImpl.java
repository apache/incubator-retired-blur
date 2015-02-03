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
package org.apache.blur.thrift.sasl;

import static org.apache.blur.utils.BlurConstants.BLUR_SECUTIRY_SASL_CUSTOM_CLASS;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

import javax.security.sasl.AuthenticationException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

/**
 * The basis for this code originated in the Apache Hive Project.
 */
public class CustomAuthenticationProviderImpl extends PasswordAuthenticationProvider {

  private static final Log LOG = LogFactory.getLog(CustomAuthenticationProviderImpl.class);

  private final PasswordAuthenticationProvider _provider;

  @SuppressWarnings("unchecked")
  public CustomAuthenticationProviderImpl(BlurConfiguration configuration) {
    super(configuration);
    String className = configuration.get(BLUR_SECUTIRY_SASL_CUSTOM_CLASS);
    LOG.info("Custom provider using class [{0}]", className);
    try {
      Class<? extends PasswordAuthenticationProvider> clazz = (Class<? extends PasswordAuthenticationProvider>) Class
          .forName(className);
      Constructor<? extends PasswordAuthenticationProvider> constructor = clazz
          .getConstructor(new Class[] { BlurConfiguration.class });
      _provider = constructor.newInstance(configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void authenticate(String username, String password, InetSocketAddress address) throws AuthenticationException {
    _provider.authenticate(username, password, address);
  }

}
