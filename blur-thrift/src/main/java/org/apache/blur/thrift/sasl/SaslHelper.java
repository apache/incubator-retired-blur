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

import static org.apache.blur.utils.BlurConstants.BLUR_SECURITY_SASL_ENABLED;
import static org.apache.blur.utils.BlurConstants.BLUR_SECURITY_SASL_TYPE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;

/**
 * The basis for this code originated in the Apache Hive Project.
 */
public class SaslHelper {

  private static final Log LOG = LogFactory.getLog(SaslHelper.class);

  private static final String BLUR_SECURITY_SASL_PLAIN_PASSWORD = "blur.security.sasl.plain.password";
  private static final String BLUR_SECURITY_SASL_PLAIN_USERNAME = "blur.security.sasl.plain.username";
  private static final String CUSTOM = "CUSTOM";
  private static final String PLAIN = "PLAIN";

  static {
    java.security.Security.addProvider(new PlainSaslServer.SaslPlainProvider());
  }

  public static TTransport getTSaslClientTransport(BlurConfiguration configuration, TTransport transport)
      throws IOException {
    AuthenticationType type = getValueOf(configuration.get(BLUR_SECURITY_SASL_TYPE));
    switch (type) {
    case ANONYMOUS:
    case LDAP:
    case CUSTOM:
      return getPlainTSaslClientTransport(type, configuration, transport);
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }

  private static TTransport getPlainTSaslClientTransport(AuthenticationType type, BlurConfiguration configuration,
      TTransport transport) throws IOException {
    final String username;
    final String password;
    switch (type) {
    case ANONYMOUS: {
      username = "anonymous";
      password = "anonymous";
      break;
    }
    case LDAP:
    case CUSTOM: {
      username = configuration.get(BLUR_SECURITY_SASL_PLAIN_USERNAME);
      password = configuration.get(BLUR_SECURITY_SASL_PLAIN_PASSWORD);
      break;
    }
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
    if (username == null) {
      throw new IOException("Username cannot be null set property [" + BLUR_SECURITY_SASL_PLAIN_USERNAME
          + "] in the BlurConfiguration.");
    }
    if (password == null) {
      throw new IOException("Password cannot be null set property [" + BLUR_SECURITY_SASL_PLAIN_PASSWORD
          + "] in the BlurConfiguration.");
    }
    final String authorizationId = null;
    final String serverName = null;
    Map<String, String> props = new HashMap<String, String>();
    CallbackHandler cbh = new CallbackHandler() {
      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
          if (callbacks[i] instanceof NameCallback) {
            NameCallback nameCallback = (NameCallback) callbacks[i];
            nameCallback.setName(username);
          } else if (callbacks[i] instanceof PasswordCallback) {
            PasswordCallback passCallback = (PasswordCallback) callbacks[i];
            passCallback.setPassword(password.toCharArray());
          } else {
            throw new UnsupportedCallbackException(callbacks[i]);
          }
        }
      }

    };
    return new TSaslClientTransport(PLAIN, authorizationId, CUSTOM, serverName, props, cbh, transport);
  }

  public static TSaslServerTransport.Factory getTSaslServerTransportFactory(BlurConfiguration configuration)
      throws IOException {
    AuthenticationType type = getValueOf(configuration.get(BLUR_SECURITY_SASL_TYPE));
    LOG.info("Setting SASL Server with authentication type [{0}]", type);
    switch (type) {
    case ANONYMOUS:
    case LDAP:
    case CUSTOM:
      return getPlainTSaslServerTransportFactory(type, configuration);
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }

  private static AuthenticationType getValueOf(String t) throws IOException {
    if (t == null) {
      return null;
    }
    String upperCase = t.toUpperCase();
    AuthenticationType[] values = AuthenticationType.values();
    for (AuthenticationType type : values) {
      if (type.name().equals(upperCase)) {
        return type;
      }
    }
    throw new IOException("Type [" + t + "] not found. Choose from [" + Arrays.asList(AuthenticationType.values())
        + "]");
  }

  private static TSaslServerTransport.Factory getPlainTSaslServerTransportFactory(final AuthenticationType type,
      final BlurConfiguration configuration) throws IOException {
    TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
    final String serverName = null;
    final Map<String, String> props = new HashMap<String, String>();
    final PasswordAuthenticationProvider provider = getAuthenticationProvider(type, configuration);
    CallbackHandler cbh = new CallbackHandler() {

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        String username = null;
        String password = null;
        AuthorizeCallback ac = null;
        for (int i = 0; i < callbacks.length; i++) {
          if (callbacks[i] instanceof NameCallback) {
            NameCallback nc = (NameCallback) callbacks[i];
            username = nc.getName();
          } else if (callbacks[i] instanceof PasswordCallback) {
            PasswordCallback pc = (PasswordCallback) callbacks[i];
            password = new String(pc.getPassword());
          } else if (callbacks[i] instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback) callbacks[i];
          } else {
            throw new UnsupportedCallbackException(callbacks[i]);
          }
        }
        InetSocketAddress address = TSaslTransport._currentConnection.get();
        provider.authenticate(username, password, address);
        if (ac != null) {
          ac.setAuthorized(true);
        }
      }
    };
    saslTransportFactory.addServerDefinition(PLAIN, CUSTOM, serverName, props, cbh);
    return saslTransportFactory;
  }

  public static PasswordAuthenticationProvider getAuthenticationProvider(AuthenticationType type,
      BlurConfiguration configuration) throws IOException {
    LOG.info("Setting SASL Server with password authentication provider [{0}]", type);
    switch (type) {
    case ANONYMOUS:
      return new AnonymousAuthenticationProviderImpl(configuration);
    case CUSTOM:
      return new CustomAuthenticationProviderImpl(configuration);
    case LDAP:
      return new LdapAuthenticationProviderImpl(configuration);
    default:
      throw new IOException("Unsupported authentication method [" + type + "]");
    }
  }

  public static void setPlainUsername(BlurConfiguration configuration, String username) {
    configuration.set(BLUR_SECURITY_SASL_PLAIN_USERNAME, username);
  }

  public static void setPlainPassword(BlurConfiguration configuration, String password) {
    configuration.set(BLUR_SECURITY_SASL_PLAIN_PASSWORD, password);
  }

  public static boolean isSaslEnabled(BlurConfiguration configuration) {
    return configuration.getBoolean(BLUR_SECURITY_SASL_ENABLED, false);
  }
}
