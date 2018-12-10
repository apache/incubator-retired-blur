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

import static org.apache.blur.utils.BlurConstants.BLUR_SECURITY_SASL_LDAP_BASEDN;
import static org.apache.blur.utils.BlurConstants.BLUR_SECURITY_SASL_LDAP_DOMAIN;
import static org.apache.blur.utils.BlurConstants.BLUR_SECURITY_SASL_LDAP_URL;

import java.io.Console;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.sasl.AuthenticationException;

import org.apache.blur.BlurConfiguration;

/**
 * The basis for this code originated in the Apache Hive Project.
 */
public class LdapAuthenticationProviderImpl extends PasswordAuthenticationProvider {

  private final String _ldapURL;
  private final String _baseDN;
  private final String _ldapDomain;

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("<ldap uri> <username>");
      System.exit(1);
    }
    String ldap = args[0];
    StringBuilder builder = new StringBuilder();
    for (int i = 1; i < args.length; i++) {
      if (builder.length() != 0) {
        builder.append(' ');
      }
      builder.append(args[i]);
    }
    String user = builder.toString();

    Console cons;

    if ((cons = System.console()) != null) {
      char[] passwd = cons.readPassword("%s", "Type Password:\n");
      BlurConfiguration blurConfiguration = new BlurConfiguration();
      blurConfiguration.set(BLUR_SECURITY_SASL_LDAP_URL, ldap);
      LdapAuthenticationProviderImpl ldapAuthenticationProviderImpl = new LdapAuthenticationProviderImpl(
          blurConfiguration);
      ldapAuthenticationProviderImpl.authenticate(user, new String(passwd), null);
      System.out.println("Valid");
    } else {
      System.err.println("No Console.");
      System.exit(1);
    }
  }

  public LdapAuthenticationProviderImpl(BlurConfiguration configuration) {
    super(configuration);
    _ldapURL = configuration.get(BLUR_SECURITY_SASL_LDAP_URL);
    _baseDN = configuration.get(BLUR_SECURITY_SASL_LDAP_BASEDN);
    _ldapDomain = configuration.get(BLUR_SECURITY_SASL_LDAP_DOMAIN);
  }

  @Override
  public void authenticate(String username, String password, InetSocketAddress address) throws AuthenticationException {

    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, _ldapURL);

    // If the domain is supplied, then append it. LDAP providers
    // like Active Directory use a fully qualified user name like foo@bar.com.
    if (_ldapDomain != null) {
      username = username + "@" + _ldapDomain;
    }

    // setup the security principal
    final String bindDN;
    if (_baseDN != null) {
      bindDN = "uid=" + username + "," + _baseDN;
    } else {
      bindDN = username;
    }
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, bindDN);
    env.put(Context.SECURITY_CREDENTIALS, password);

    try {
      // Create initial context
      DirContext ctx = new InitialDirContext(env);
      ctx.close();
    } catch (NamingException e) {
      throw new AuthenticationException("Error validating LDAP user", e);
    }
    return;
  }
}
